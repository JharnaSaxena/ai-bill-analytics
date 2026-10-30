<h1>AI Bill Analytics</h1>
<p><b>Phase 1 — Lambda Architecture for LLM Cost and Savings Analytics</b><br>
Jharna Saxena · Rishika Singh · 30 October 2026</p>

<h2>1. Problem &amp; Objectives</h2>
<p>Companies using LLMs overspend sometimes a retry loop can burn thousands before anyone notices — and have no per-team cost attribution for chargeback. This project builds a Lambda pipeline that (i) tracks tokens and cost per team/model, (ii) fires a budget alert within seconds at 80%/100%, and (iii) produces an exact, deduplicated daily bill reconcilable with the provider invoice.</p>

<h2>2. Architecture</h2>
<p><b>Lambda</b>, not Kappa. Batch gives exact, idempotent figures; streaming gives fast, approximate alerts. Kappa would replay the whole log on every correction slower and costlier.</p>

<p><b>Flow:</b> three sources → Python producer (S3 first, Kafka second) → batch (PySpark) and streaming (Spark Structured Streaming) → S3 views → Glue catalog → Athena serving layer → Grafana dashboard.</p>

<p><b>Master–slave.</b> S3 is the master (append-only, never rewritten). <code>batch_view</code>, <code>speed_view</code>, <code>batch_meta</code> are slaves derived from it. <code>merged_view</code> is an Athena view over them. A view cannot be written to, so nothing can be duplicated through the serving path.</p>

<h2>3. Data Sources</h2>
<table>
<tr><th>Source</th><th>Format</th><th>Type</th><th>Columns</th></tr>
<tr><td>LLM Usage Events</td><td>JSON</td><td>Semi-structured</td><td><code>event_id, request_id, event_ts, team_id, app_id, model, tokens_in, tokens_out, status</code></td></tr>
<tr><td>Provider Usage Reports</td><td>JSON</td><td>Semi-structured, 5s–10min late</td><td><code>request_id, billed_tokens, latency_ms, status, reported_at</code></td></tr>
<tr><td>Price / Budget / Policy</td><td>CSV</td><td>Structured, effective-dated</td><td><code>model, input_price_per_1k, output_price_per_1k, team_id, budget_limit, effective_from, effective_to</code></td></tr>
</table>

<p>Cost is <b>not</b> stored in the event. It is computed in shared code from the effective-dated price table, so a price change is applied by recomputing batch, not by rewriting history.</p>

<h2>4. Storage Layout (single S3 bucket, five prefixes)</h2>
<table>
<tr><th>Prefix</th><th>Contents</th><th>Rewritten?</th></tr>
<tr><td><code>raw/</code></td><td>Append-only master. Parquet, <code>year=/month=/day=/hour=/</code></td><td>Never</td></tr>
<tr><td><code>batch/</code></td><td>Hourly aggregates (authoritative)</td><td>Per hour, on recompute</td></tr>
<tr><td><code>speed/</code></td><td>1-minute aggregates (approximate)</td><td>Per minute</td></tr>
<tr><td><code>meta/</code></td><td><code>batch_meta</code>: <code>max_event_ts</code> per <code>(team_id, model)</code>, <code>status</code></td><td>Per hour</td></tr>
<tr><td><code>dead-letter/</code></td><td>Failed events (missing id, negative tokens, unknown model)</td><td>Never</td></tr>
</table>

<h2>5. Idempotency (three mechanisms)</h2>
<ol>
<li><b>Dedup on load</b> — <code>dropDuplicates("event_id")</code> inside each batch.</li>
<li><b>Watermark check</b> — skip rows whose <code>event_ts</code> ≤ stored <code>max_event_ts</code> for that <code>(team_id, model)</code>.</li>
<li><b>Partition overwrite</b> — re-running hour 09 replaces hour 09's files, never appends.</li>
</ol>
<p>Parquet on S3 has no unique constraint, so overwrite is what guarantees idempotency — no database required.</p>

<h2>6. Batch Checklist (Lambda)</h2>
<p><b>Split-write fault tolerance.</b> S3 first. If Kafka fails, event is safe in S3; producer retries 3×; a repair job compares the per-minute <code>event_id</code> list to S3 and re-sends. Reverse case cannot occur — S3 write must succeed before Kafka.</p>
<p><b>Consistency.</b> Both jobs import the same <code>transformations.py</code> (dedup, join, pricing, aggregation). Same schema contract on both sides.</p>
<p><b>Serving.</b> <code>merged_view</code> in Athena: batch for hours &lt; <code>max(hour_ts)</code> in <code>batch_meta</code> where status='complete', streaming for the rest. Time ranges disjoint → no double count. <code>batch_meta</code> is itself a serving-layer table, so the boundary is never stale.</p>

<h2>7. Streaming</h2>
<p>Spark Structured Streaming keeps a stateful running total per team, uses <b>sliding windows</b> for spike detection, and fires alerts at 80%/100%. Writes a 1-minute aggregate to <code>speed/</code> every minute. Late events past the window are dropped from streaming but recovered by the next batch recompute.</p>
<p><b>Why sliding, not tumbling:</b> a spike often straddles a minute boundary — tumbling splits it in two; sliding evaluates the recent interval as a whole. During steady weekday traffic they produce identical totals; at night and weekends both report zero.</p>

<h2>8. Serving Layer — Athena, not PostgreSQL</h2>
<p>PostgreSQL has a write path that can create duplicates. <b>Athena is read-only</b> — it queries, never writes. The guidelines list Athena as a valid serving store. <code>batch_meta</code> is cataloged in Glue alongside the two views so <code>merged_view</code> reads a fresh boundary on every query.</p>

<h2>9. Infrastructure</h2>
<p>Single Ubuntu EC2 <code>t3.xlarge</code> (4 vCPU / 16 GB / 50 GB gp3), one Docker Compose project. All versions pinned. <code>$100</code> Learner Lab budget; instance stopped between sessions. Five containers: Kafka, PySpark, Spark Streaming, Grafana, Python producer. No database container.</p>

<table>
<tr><th>Port</th><th>Service</th><th>Reachability</th></tr>
<tr><td>22</td><td>SSH</td><td>Team IP /32 only</td></tr>
<tr><td>3000</td><td>Grafana</td><td>Team IP /32 only</td></tr>
<tr><td>9092</td><td>Kafka</td><td>Internal only</td></tr>
<tr><td>4040</td><td>Spark UI</td><td>Internal only</td></tr>
</table>
<p>Athena and S3 are outbound HTTPS only — no inbound rule. <code>.env</code> in <code>.gitignore</code>. Credentials from EC2 instance profile.</p>

<h2>10. Open Items</h2>
<ul>
<li>Athena access in the Learner Lab pending confirmation. Fallback: DuckDB over S3.</li>
<li>Measured event size (assumed 1 KB).</li>
<li>Actual AWS cost after week 1.</li>
</ul>

<p><b>Status:</b> Phase 1 design. Implementation and demo due 20 November 2026.</p>
