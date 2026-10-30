<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Phase 1 — AI Bill Analytics</title>
</head>
<body>

<header>
  <h1>Phase 1 — AI Bill Analytics</h1>
  <h2>Lambda Architecture for LLM Cost and Savings Analytics</h2>
  <div class="meta">
    <span><strong>Jharna Saxena</strong> and <strong>Rishika Singh</strong></span>
    <span>Big Data Analytics with Hadoop and Spark (CS1243)</span>
    <span>Submitted to Deepika Prakash Ma'am</span>
    <span><a href="https://github.com/JharnaSaxena/ai-bill-analytics">github.com/JharnaSaxena/ai-bill-analytics</a></span>
    <span>30 October 2026</span>
  </div>
</header>

<!-- ====================================================== -->
<h2 class="section">1. Project Overview</h2>

<h3>1.1 Problem</h3>
<p>Companies using LLMs face two main cost challenges: (1) spending can increase unexpectedly before anyone notices; (2) provider invoices show total costs, but managers need a breakdown by team and model for chargeback.</p>

<h3>1.2 Objectives</h3>
<ul>
  <li>Track token usage and costs by team, model and application.</li>
  <li>Monitor live spending and trigger alerts at 80% and 100% of the budget.</li>
  <li>Generate accurate daily bills by removing duplicates and correcting late-arriving events.</li>
  <li>Display month-to-date spending and remaining budget on a single dashboard.</li>
</ul>

<h3>1.3 Assumptions</h3>
<p>The system handles 50 data points/sec by default. Provider reports may arrive a few seconds late. Streaming uses nearly a 1-second time window, with alerts expected within a few seconds.</p>

<!-- ====================================================== -->
<h2 class="section">2. Architecture Design</h2>

<h3>2.1 Lambda vs Kappa</h3>
<p>We chose Lambda because batch and streaming have different requirements. Batch processing needs accurate, repeatable results, including late data corrections, while streaming prioritises speed and can use approximate results. With Kappa, each correction would require replaying the log, increasing processing time and cost.</p>

<h3>2.2 High-Level Architecture</h3>
<p>Three sources write events to S3 and Kafka. Kafka feeds the streaming layer, while S3 acts as the source for batch processing.</p>
<ul>
  <li><strong>Batch:</strong> PySpark reads S3, removes duplicates, joins data and applies pricing rules. It recomputes hourly aggregates and writes results to <code>s3://.../batch/</code>. Processing watermarks are stored in <code>s3://.../meta/</code>.</li>
  <li><strong>Streaming:</strong> Spark Streaming maintains running totals per team and uses sliding windows to detect spending spikes. Results are written to <code>s3://.../speed/</code> every minute.</li>
</ul>
<p>Glue catalogs <code>batch/</code>, <code>speed/</code> and <code>meta/</code> as tables. Athena provides <code>batch_view</code>, <code>speed_view</code> and <code>batch_meta</code>, along with a <code>merged_view</code> that combines both layers using the boundary rule. Grafana uses this view for the dashboard. Athena serves as the query layer, so no separate database is needed.</p>

<h3>2.3 Flowchart</h3>
<figure>
  <img src="arch.png" alt="Proposed Lambda pipeline diagram">
  <figcaption><strong>Figure 1.</strong> Proposed Lambda pipeline. Three sources feed a Python producer that performs the split write (S3 first, then Kafka). The S3 batch path recomputes authoritative hourly aggregates; the Kafka speed path serves approximate one-minute windows. Glue catalogs the three S3 prefixes as tables, Athena exposes <code>batch_view</code>, <code>speed_view</code> and <code>batch_meta</code>, and <code>merged_view</code> applies the boundary rule. Grafana reads <code>merged_view</code> over the Athena plugin.</figcaption>
</figure>

<h3>2.4 Master–Slave</h3>
<p>The raw data in S3 is the master copy. It is append-only and is never modified. The batch, speed and metadata views are derived from this data. Athena is read-only, so the serving layer cannot introduce duplicate records.</p>

<h3>2.5 Storage Tiers</h3>
<ul>
  <li><code>raw/</code> — Append-only Parquet files, partitioned by year, month, day and hour. Minute and second are stored as columns.</li>
  <li><code>batch/</code> — Hourly aggregates, overwritten when an hour is recomputed.</li>
  <li><code>speed/</code> — One output file per minute.</li>
  <li><code>meta/</code> — Processing watermarks stored per team.</li>
  <li><code>dead-letter/</code> — Rejected events, stored separately and never overwritten.</li>
</ul>

<!-- ====================================================== -->
<h2 class="section">3. Data Sources and Event Contract</h2>

<h3>3.1 Data Sources and Column Names</h3>
<table>
  <thead>
    <tr><th style="width:22%">Source</th><th style="width:8%">Format</th><th style="width:16%">Type</th><th>Columns</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>LLM Usage Events</td>
      <td>JSON</td>
      <td>Semi-structured</td>
      <td><code>event_id</code>, <code>request_id</code>, <code>event_ts</code>, <code>team_id</code>, <code>app_id</code>, <code>model</code>, <code>tokens_in</code>, <code>tokens_out</code>, <code>status</code></td>
    </tr>
    <tr>
      <td>Provider Usage Reports</td>
      <td>JSON</td>
      <td>Semi-structured</td>
      <td><code>request_id</code>, <code>billed_tokens</code>, <code>status</code>, <code>reported_at</code></td>
    </tr>
    <tr>
      <td>Price / Budget / Policy</td>
      <td>CSV</td>
      <td>Structured (effective-dated)</td>
      <td><code>model</code>, <code>input_price_per_1k</code>, <code>output_price_per_1k</code>, <code>team_id</code>, <code>budget_limit</code>, <code>effective_from</code>, <code>effective_to</code></td>
    </tr>
  </tbody>
</table>
<p><code>event_id</code> is the dedup key. <code>request_id</code> links a usage event to its later provider report. <code>event_ts</code> is event time, not ingestion time. Cost is not stored in the event; it is computed from the effective-dated price table in shared code, so a price change is applied by recomputing batch, not by rewriting history.</p>

<h3>3.2 Validation and Idempotency</h3>
<p>Every event is checked against the schema in a shared Python module. Bad events (missing ID, negative tokens, unknown model) go to <code>s3://.../dead-letter/</code> and are counted on Grafana.</p>
<p>Idempotency is enforced at write time in two steps:</p>
<ol>
  <li><strong>Dedup on load:</strong> <code>dropDuplicates(event_id)</code> in PySpark removes duplicates inside one batch.</li>
  <li><strong>Watermark + partition overwrite:</strong> for each <code>(team_id, model)</code>, read the stored <code>max(event_ts)</code> from <code>batch_meta</code>, keep only rows with <code>event_ts</code> greater, and write with dynamic partition overwrite so re-running an hour replaces that hour's files instead of appending.</li>
</ol>

<!-- ====================================================== -->
<h2 class="section">4. Technology Justification</h2>

<h3>4.1 Ingestion: Kafka</h3>
<p>Single durable log, 7-day retention, replayable. Runs inside Docker Compose, no external dependency, replays support reprocessing.</p>

<h3>4.2 Batch: PySpark</h3>
<p>Dedup, join on <code>request_id</code>, effective-dated pricing, watermark check, hourly recompute. Shares <code>transformations.py</code> with streaming so both paths use the same formulas. Partition overwrite makes Parquet-on-S3 idempotent without a database.</p>

<h3>4.3 Storage: S3 + Parquet</h3>
<p>Append-only master at <code>raw/</code>, partitioned <code>year=/month=/day=/hour=/</code>. Parquet reduces Athena scan cost. The daily bill reads <code>batch_view</code> only — a finished day is fully covered by batch, so no merged view is needed. The live dashboard reads <code>merged_view</code> because it spans completed hours and the in-progress hour.</p>

<h3>4.4 Catalog: Glue + Athena</h3>
<p>Glue holds the schema; Athena runs SQL over S3. Serverless, no copy step, partition projection keeps cost low.</p>

<h3>4.5 Streaming: Spark Structured Streaming</h3>
<p>Stateful job keeping a running total of tokens and spend per team, with <strong>sliding windows</strong> for spike detection. Alerts fire when the total crosses 80% / 100%.</p>
<p><strong>Why sliding windows:</strong> We use sliding windows to detect spending spikes across the week. Assuming office hours of Monday–Friday, 9 AM–5 PM, usage is expected to be low at nights and weekends. A sliding-window approach is sufficient, without separate tumbling computations. Streaming state is checkpointed to disk to recover running totals after restarts.</p>
<p>A small scheduler checks <code>batch_meta</code> once per hour. If any hour has <code>status = 'incomplete'</code> or has new rows in <code>raw/</code> that arrived after its <code>max_event_ts</code>, that hour is re-queued and re-run automatically.</p>

<h3>4.6 Serving Layer: Athena</h3>
<p>Glue catalogs <code>batch/</code>, <code>speed/</code> and <code>meta/</code>; Athena exposes them as <code>batch_view</code>, <code>speed_view</code> and <code>batch_meta</code>, plus <code>merged_view</code>. Grafana reads <code>merged_view</code> through the Athena plugin.</p>
<p><strong>Why Athena instead of PostgreSQL:</strong> Using PostgreSQL would introduce another write layer, requiring additional handling to prevent duplicate records. Athena is read-only, so all writes remain on S3, where partition overwrite and watermark checks handle duplicates.</p>
<p><strong><code>batch_meta</code> in the serving layer:</strong> It stores the latest completed batch boundary. Cataloging it in Glue allows <code>merged_view</code> to use the updated boundary on each query, preventing the same hour from appearing in both batch and streaming results.</p>

<h3>4.7 Visualisation: Grafana (3000)</h3>
<p>Reads <code>merged_view</code> through the Athena plugin. Time-series dashboards, built-in alerting, dashboard export as JSON for reproducibility.</p>

<!-- ====================================================== -->
<h2 class="section">5. Lambda Validation</h2>

<h3>5.1 Split-Write Fault Tolerance</h3>
<p><strong>S3 ok, Kafka fails:</strong> event safe in S3; producer retries Kafka three times. If all retries fail, the <code>event_id</code> is logged; a repair job compares <code>event_id</code> sets between the per-minute list and S3 and re-sends from S3.</p>
<p><strong>Kafka ok, S3 fails:</strong> cannot happen, because S3 is written first. If the S3 write fails, the producer does not proceed to Kafka.</p>

<h3>5.2 Batch–Stream Consistency</h3>
<p>Both jobs import the same <code>transformations.py</code> and validate against the same schema contract. A formula change is made in one file.</p>

<h3>5.3 Serving Strategy</h3>
<ul>
  <li>Batch writes hourly aggregates to <code>batch/</code> and the watermark to <code>meta/</code>. Authoritative for completed hours.</li>
  <li>Streaming writes one-minute aggregates to <code>speed/</code>. Fills only the gap after the last completed batch hour.</li>
  <li>Grafana reads <code>merged_view</code>, which reads <code>batch_meta</code> to locate the boundary on every query.</li>
</ul>

<h3>5.4 Double-Counting Prevention</h3>
<p><em>cutoff_ts</em> marks the last hour that batch has fully processed. Batch covers every hour before it; streaming covers everything after it. The two ranges never touch, so no hour is counted twice. If an hour fails, the cutoff does not move, so streaming keeps covering it. When the hour succeeds, it is written first and the cutoff is updated second, so readers never see both at once.</p>
<p>For example, at 10:02 the cutoff is 09:00. Batch serves everything before 09:00; streaming serves 09:00–10:02. When the 09:00 batch finishes, the cutoff moves to 10:00 and the merged view switches to the batch result.</p>
<p>Double counting is prevented by two rules.</p>
<p><strong>Rule 1 — split the time into two disjoint parts.</strong> The <em>merged view</em> does not mix batch and streaming rows. For any hour that a completed batch has already covered, it takes only the batch row. For the time after that hour, it takes only the streaming row. Because the two time ranges never overlap, no event can appear in both.</p>
<p><strong>Rule 2 — make batch rewrites safe.</strong> Before writing, PySpark removes duplicate <code>event_id</code>s inside the batch, then skips any row whose <code>event_ts</code> is not greater than the stored <code>max(event_ts)</code> for that <code>(team_id, model)</code>. Finally it overwrites only the partition being recomputed. Re-running the same hour therefore replaces that hour's files, not appends a second copy. The boundary itself comes from <code>batch_meta</code>, cataloged in Glue alongside <code>batch_view</code> and <code>speed_view</code>. Because it sits inside the serving layer, every query reads the current boundary. If <code>batch_meta</code> were kept outside, the view would use a stale value after batch advanced it, and that hour would briefly appear in both branches.</p>

<h3>5.5 Late Data and Reconciliation</h3>
<p>Some reports arrive after the streaming window has already closed. Streaming does not go back, so it simply does not see them. That is acceptable, because streaming is only approximate.</p>
<p>The report is not lost. It stays in <code>s3://.../raw/</code> forever. The next batch recompute of that hour joins it on <code>request_id</code> and includes it in the batch output.</p>
<p>When that recompute finishes, the row for that hour in <code>batch_meta</code> is updated. Then the boundary moves forward. The next <code>merged_view</code> query uses the new batch value for that hour and stops including the older streaming value for it. The corrected figure appears on the dashboard on the next refresh.</p>

<!-- ====================================================== -->
<h2 class="section">6. Infrastructure</h2>

<h3>6.1 Deployment</h3>
<p>One Ubuntu EC2 <code>t3.xlarge</code> (4 vCPU / 16 GB RAM), 50 GB gp3, one <code>docker compose up -d</code>. All versions pinned. Instance stopped between sessions.</p>

<h3>6.2 Services</h3>
<p>Kafka, PySpark batch, Spark Streaming, Grafana, Python producer. No database container; Athena and S3 are managed AWS services reached over HTTPS.</p>

<h3>6.3 Ports</h3>
<table>
  <thead><tr><th style="width:30%">Service</th><th style="width:20%">Port</th><th>Reachable from</th></tr></thead>
  <tbody>
    <tr><td>SSH</td><td>22</td><td>Team public IP <code>/32</code> only</td></tr>
    <tr><td>Grafana</td><td>3000</td><td>Team public IP <code>/32</code> only</td></tr>
    <tr><td>Kafka</td><td>9092</td><td>Internal Docker network only</td></tr>
    <tr><td>Spark UI</td><td>4040</td><td>Internal Docker network only</td></tr>
  </tbody>
</table>

<h3>6.4 AWS Resources</h3>
<p>S3 bucket with five prefixes: <code>raw/</code>, <code>batch/</code>, <code>speed/</code>, <code>meta/</code>, <code>dead-letter/</code>. Glue catalog, Athena, Grafana and EC2 instance profile for credentials. <code>.env</code> is in <code>.gitignore</code>.</p>

</body>
</html>
