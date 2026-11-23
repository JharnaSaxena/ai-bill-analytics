from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

USAGE_EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),    True),
    StructField("request_id", StringType(),    True),
    StructField("event_ts",   TimestampType(), True),
    StructField("team_id",    StringType(),    True),
    StructField("app_id",     StringType(),    True),
    StructField("model",      StringType(),    True),
    StructField("tokens_in",  LongType(),      True),
    StructField("tokens_out", LongType(),      True),
    StructField("status",     StringType(),    True),
])

PRICE_SCHEMA = StructType([
    StructField("model",               StringType(), False),
    StructField("team_id",             StringType(), False),
    StructField("input_price_per_1k",  StringType(), False),
    StructField("output_price_per_1k", StringType(), False),
    StructField("budget_limit",        StringType(), False),
    StructField("effective_from",      StringType(), False),
    StructField("effective_to",        StringType(), False),
])

SLOT_A_START_HOUR = 9
SLOT_A_END_HOUR   = 13
SLOT_B_START_HOUR = 13
SLOT_B_END_HOUR   = 17


def load_prices(spark, path):
    return (spark.read.option("header", True).schema(PRICE_SCHEMA).csv(path)
            .withColumn("input_price_per_1k",  F.col("input_price_per_1k").cast("double"))
            .withColumn("output_price_per_1k", F.col("output_price_per_1k").cast("double"))
            .withColumn("budget_limit",        F.col("budget_limit").cast("double"))
            .withColumn("effective_from",      F.to_date("effective_from"))
            .withColumn("effective_to",        F.to_date("effective_to")))


def dedup(df):
    return df.dropDuplicates(["event_id"]).filter(F.col("event_id").isNotNull())


def add_slot_start(df):
    h = F.hour(F.col("event_ts"))
    d = F.to_date(F.col("event_ts"))
    return df.withColumn(
        "slot_start",
        F.when((h >= SLOT_A_START_HOUR) & (h < SLOT_A_END_HOUR),
               F.to_timestamp(F.concat(d, F.lit(" 09:00:00"))))
         .when((h >= SLOT_B_START_HOUR) & (h < SLOT_B_END_HOUR),
               F.to_timestamp(F.concat(d, F.lit(" 13:00:00"))))
         .otherwise(F.lit(None).cast("timestamp")))


def filter_valid(df, prices):
    known = prices.select("model").distinct()
    return (df
            .filter(F.col("event_ts").isNotNull())
            .filter(F.col("tokens_in")  >= 0)
            .filter(F.col("tokens_out") >= 0)
            .join(known, on="model", how="left_semi"))


def split_valid_invalid(df, prices):
    known = prices.select("model").distinct()
    annotated = (df
        .withColumn("_r1", F.when(F.col("event_id").isNull(), F.lit("missing_event_id")))
        .withColumn("_r2", F.when((F.col("tokens_in") < 0) | (F.col("tokens_out") < 0), F.lit("negative_tokens")))
        .withColumn("_r3", F.when(F.col("event_ts").isNull(), F.lit("null_ts")))
        .withColumn("reject_reason", F.coalesce("_r1", "_r2", "_r3"))
        .drop("_r1", "_r2", "_r3"))
    annotated = (annotated
        .join(known.withColumn("_known", F.lit(True)), on="model", how="left")
        .withColumn("reject_reason",
                    F.when(F.col("_known").isNull() & F.col("reject_reason").isNull(),
                           F.lit("unknown_model")).otherwise(F.col("reject_reason")))
        .drop("_known"))
    annotated = annotated.withColumn(
        "reject_reason",
        F.when(F.col("reject_reason").isNull() &
               ((F.hour(F.col("event_ts")) < SLOT_A_START_HOUR) |
                (F.hour(F.col("event_ts")) >= SLOT_B_END_HOUR)),
               F.lit("outside_office_hours"))
         .otherwise(F.col("reject_reason")))
    valid   = annotated.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    invalid = annotated.filter(F.col("reject_reason").isNotNull())
    return valid, invalid


def with_cost(df, prices):
    priced = (df
              .withColumn("event_date", F.to_date("event_ts"))
              .join(prices, on=["model", "team_id"], how="inner")
              .filter((F.col("event_date") >= F.col("effective_from")) &
                      (F.col("event_date") <= F.col("effective_to"))))
    return priced.withColumn("cost",
        (F.col("tokens_in")  / 1000.0) * F.col("input_price_per_1k") +
        (F.col("tokens_out") / 1000.0) * F.col("output_price_per_1k"))


def current_slot_start_utc():
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    d = now.strftime("%Y-%m-%d")
    h = now.hour
    if h < SLOT_B_START_HOUR: return f"{d} 09:00:00"
    if h < SLOT_B_END_HOUR:   return f"{d} 13:00:00"
    return                          f"{d} 17:00:00"