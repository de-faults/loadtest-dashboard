# Loadtest Dashboard

Grafana-styled load-testing control panel. Three protocols, everything configurable
from the UI, live metrics over SSE, English/Thai interface, CSV report export.

| Protocol | Engine | Notes |
|---|---|---|
| REST | [k6](https://k6.io) | external binary, live NDJSON metrics |
| WebSocket | [Artillery](https://artillery.io) | raw `ws` or Socket.IO; external binary + bundled live-stats plugin |
| Kafka | `@confluentinc/kafka-javascript` | in-process, raw librdkafka properties |

## Quick start

```bash
npm install          # also installs web/
npm run build        # tsc + vite build
npm start            # http://127.0.0.1:4300
```

Development (two processes, Vite proxies `/api` and `/events` to the server):

```bash
npm run dev          # API on :4300
npm run dev:web      # UI on :4301
```

### Runner prerequisites

k6 and Artillery are external binaries. The dashboard detects them at startup and
shows each runner's status in **Settings → Runner availability**; a missing binary
disables that protocol instead of failing a run halfway.

```bash
brew install k6                 # or https://k6.io/docs/get-started/installation/
npm install -g artillery        # or point Settings at ./node_modules/.bin/artillery
```

Binary paths are editable in the UI, so a non-PATH install needs no env vars.

### Local targets for trying it out

```bash
npm run target:http      # HTTP on :4399, fails every 25th request
npm run target:ws        # WebSocket echo on :4398
npm run target:socketio  # Socket.IO on :4396, replies via ack and event
```

Kafka in one line:

```bash
docker run -d --name ltd-kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  apache/kafka:3.9.0
```

## How the metrics work

Every runner emits the same `RunEvent` shape (`src/shared/types.ts`), so the UI,
threshold engine and CSV exporter are protocol-agnostic.

**Percentiles.** Latency goes into a log-linear histogram (`src/metrics/histogram.ts`,
~0.1% error, bounded memory). Whole-run percentiles come from *merging* histograms —
never from averaging per-window percentiles, which is not a valid operation. The
histogram is persisted per run, so exact percentiles stay recomputable.

**Throughput.** `rps_avg` / `tps_avg` are derived from totals ÷ duration, not from the
mean of per-second windows: a partial trailing window would drag the mean below the
real number.

RPS and TPS are separate measurements, not two labels for one number. RPS counts
HTTP requests; TPS counts completed transactions (k6 iterations). A scenario that
issues three requests per iteration reports roughly 3× the RPS of its TPS. Where a
runner has no distinct notion of a transaction — one Kafka message, one socket
emit — the two are equal by definition and the chart plots a single line.

**Per protocol:**

- **REST** — k6 streams `--out json` to a file which the server tails. Latency and
  pass/fail come from the same `http_req_duration` point (k6 tags it with
  `expected_response`). k6's `handleSummary` output is authoritative for its own
  thresholds, and its exit code (99 = threshold breach) can independently fail the run.
- **WebSocket / Socket.IO** — two engines. Raw `ws` sends plain frames. Socket.IO
  sends named events and optionally waits for the server's acknowledgement
  callback; assertions are a JSON path plus expected value. A profile can hold
  several named scenarios sharing one connection setup and one set of phases —
  each virtual user runs exactly one of them, picked by `weight`, so the
  scenarios split the load instead of multiplying it. Waiting on a
  server-pushed event is deliberately not offered: the engine records a flat ~10s
  for every such step whether or not the event arrives, and it cannot be combined
  with an acknowledgement, so it would corrupt the latency distribution. An
  imported script using one is reported as a warning. With acknowledgements the run reports a genuine emit→ack round trip
  (`socketio.response_time`); without one, latency falls back to
  `vusers.session_length` — whole-scenario duration, think time included — and the
  two are never mixed into the same distribution. Artillery only writes `--output` at the end, so live charts come from
  a bundled output plugin that POSTs each `stats` tick to a loopback ingest endpoint.
  Artillery reports quantiles rather than raw samples, so the distribution is
  reconstructed piecewise between the reported points. If the flow has no round-trip
  metric, latency falls back to `vusers.session_length` — whole-scenario duration,
  think time included — and the run log says so rather than passing it off as latency.
- **Kafka** — in-process producers with a token-bucket pacer. `produce-ack` mode times
  `send()` resolution. `end-to-end` mode stamps `ltd-sent-at` in a header and times the
  consumer's receipt; it first waits for the consumer group to actually join (a
  warm-up handshake) so rebalance time is not billed as message latency, and totals
  then count *consumed* messages. Optional consumer-lag sampling charts lag on the same
  x-axis as TPS, so lag spikes line up with load.

## Import and export scripts

Every protocol round-trips between a script file and the UI form.
**Configuration → Import / Export**:

- **Import script…** parses a file and fills in the form. The file is *never
  executed* — JavaScript is read through an AST, YAML through a parser. The
  protocol is detected from the file itself.
- **Export as script** downloads the current form as a runnable script.

| Protocol | Format | Notes |
|---|---|---|
| REST | k6 JavaScript | real k6 script; runs standalone under `k6 run` |
| WebSocket | Artillery YAML | real Artillery script; runs standalone under `artillery run` |
| Kafka | YAML config | Kafka has no standard script format, so the portable form is declarative — and carries no executable code |

Exports carry a small `dashboard` metadata block for the few things the tools
have no equivalent for (per-check minimum pass rate, body type, auth kind).
Delete it and the file still runs; those fields just fall back to defaults on
the next import.

**Foreign scripts import too.** A k6 script written by hand — no metadata block —
is read for its `options` (stages, scenarios, thresholds), its `http.*` call
(URL, method, headers, timeout, redirects, body), its `sleep()`, and any check
predicates the form can represent, including idioms like
`r.status >= 200 && r.status < 300` (→ a `200-299` range check) and
`r.timings.duration < 250`. k6 thresholds are mapped back to the DSL
(`http_req_failed: rate<0.05` → `error_rate < 5`).

Anything that cannot be represented is reported as a warning in the UI rather
than dropped silently — a dynamic body such as `JSON.stringify(payload)`, or a
predicate like `r.status % 7 === 0`.

### Running an existing file untouched

For a script with its own imports, `open()` calls or CSV data feeds, tick
**Run an existing script file** and give an absolute path. It executes where it
lives so relative paths keep resolving, and the form is ignored for that run.

## Thresholds

One DSL for all three protocols, evaluated server-side:

```
p95 < 500        success_rate > 99      rps > 1000
p99 <= 1000      error_rate < 1         tps >= 5000
avg < 200        max < 5000             total_requests > 10000
```

Kafka adds consumer-lag and volume metrics:

```
lag_max < 50000      lag_end < 1000       lag_drain_s < 60
lag_growth <= 0      total_mb >= 10000    mb_per_s > 50
```

A metric the run could not measure (no lag sampled, or `lag_drain_s` without a
drain wait) fails its threshold rather than passing quietly.

REST thresholds are additionally compiled into k6's own `options.thresholds`; if
either side fails, the run fails. A run passes only when every threshold passes **and**
every check meets its configured minimum pass rate.

## Kafka consumer lag and volume test

With **Chart consumer lag** on, the run samples the committed offsets of the
consumer group (or of every group committing on the topic, when none is named)
once a second, charts it next to TPS, and ends with a **Kafka volume & consumer
lag** panel:

| | |
|---|---|
| **start / max / avg / end of load** | backlog inherited, worst seen (and when), mean while load ran, at the moment production stopped |
| **growth** | lag added per second of load; above 0 the consumers are falling behind for good |
| **drain time** | seconds after production stopped until lag reached 0 |
| **hottest partitions** | per-partition peak and final lag — one hot partition hides inside a total |
| **volume** | messages and MB acked, MB/s average and peak second, what ended production |

**Live view.** While the run samples, the lag panel shows current / min / max /
mean / median of total lag and a per-partition table (end offset, committed,
lag) refreshed every second.

**Per-message lag.** Every acked message's partition and offset is remembered
with its send time; when the group's committed offset is seen past it, that
message's lag is recorded (ms). The run charts min / p50 / p95 / p99 / max per
second and ends with a per-group table including *pending* — messages never
consumed before the run ended. Only committed offsets are visible from outside
a consumer, so resolution is the 1 s sample plus the group's commit interval
(auto-commit defaults to 5 s). Needs `acks` 1 or all; up to 2 M unconsumed
messages are tracked at once. Thresholds: `msg_lag_avg`, `msg_lag_p50`,
`msg_lag_p95`, `msg_lag_p99`, `msg_lag_max` (worst group; any pending message
fails `msg_lag_max`, so pair it with a drain wait).

**Volume test.** Set *Volume limit (MB)* to push a fixed amount of data (key +
value bytes) instead of running for a fixed time, and *Wait for consumers to
catch up* to keep sampling after production until the group reaches lag 0. The
drain wait is not billed to throughput: in produce-ack mode RPS/TPS are averaged
over the load phase only.

**Conditions in the script.** A custom generator can carry its own run
conditions, so the profile form only needs the broker connection:

```js
export const options = {
  targetRate: 5000, maxMb: 2000, drainTimeoutSec: 120,
  consumerGroup: 'orders-service',
  thresholds: ['lag_max < 200000', 'lag_drain_s < 60', 'total_mb >= 2000'],
};
```

Recognised keys: `topic`, `targetRate`, `durationSec`, `maxMessages`, `maxMb`,
`producers`, `consumerGroup`, `monitorLag`, `latencyMode`, `drainTimeoutSec`,
`thresholds`. They override the form for that run; `thresholds` add to the
profile's. Unknown keys and bad values are logged and ignored. See
`src/runners/examples/kafka.example.mjs`. The YAML export carries `maxMb` and
`drainTimeoutSec` as well.

## Capacity (ramp) test

A capacity test walks the load up in steps and holds each step long enough for the
system to settle; the number worth taking away is not the run average but the last
step the system still served inside its SLO. Two pieces support it:

**Building the ramp.** REST → load model `stages` → *Capacity ramp builder*: start,
step, max VUs plus ramp-up and hold seconds, and it writes the stage list (ramp,
hold, ramp, hold, …, cool-down). The generated stages stay editable, so a
non-uniform ramp is still a hand edit away.

**Reading the result.** Every run gets a **Capacity** panel as soon as it has held
two distinct load levels — live, while the ramp is still climbing. It reports, per
held step: RPS, TPS, latency percentiles, worst single second, error rate, and every
threshold of the profile evaluated against that step alone. From the curve it derives:

| | |
|---|---|
| **capacity** | last step inside the SLO before the first failing one |
| **saturation** | last step before extra VUs stopped buying throughput (gain ratio < 0.3) |
| **breaking point** | first step that broke a threshold |
| **recommended** | 80 % of whichever ceiling bound first — the level to drive the follow-up performance test at |

The analysis reads the recorded 1-second windows, never the configuration, so a
bring-your-own k6 script with its own `stages` (or an Artillery arrival-rate ramp,
or a Kafka producer ramp) is analysed exactly the same way. Ramp transitions are
excluded by a minimum hold length, and the settling head of each plateau is dropped
before it is measured.

Step percentiles are request-weighted means of the per-second percentiles — the raw
distribution is not kept per step. Compare them step to step; quote the whole-run
percentiles for an SLA.

Thresholds double as the SLO, and are split by what they can say about one step. An
upper bound on latency or errors (`p95 < 500`, `success_rate > 99`) must hold at
every load level, so it decides whether the step held. A throughput or volume goal
(`rps > 1000`, `tps >= 300`) describes the run as a whole — the first steps of a ramp
are below it by design — so it is measured and shown per step, tagged *goal*, but
never counted against one. Without any gating threshold only the throughput curve is
read, and the panel says so.

RPS and TPS are separate measurements: TPS counts completed transactions, and an
async journey that submits then polls issues several requests per transaction. For a
one-request iteration the two are the same number and the chart drops the duplicate
line, while the table keeps both columns.

## CSV export

`GET /api/runs/:id/export.csv?type=summary|timeseries|checks|thresholds|errors|capacity|kafka|all`

- UTF-8 **BOM** is written — without it Excel renders Thai as mojibake.
- Fields starting with `= + - @` are prefixed with `'`. Error text comes from the
  system under test, so it is attacker-shaped.
- Delimiter (`,` `;` tab) and header language are configurable; internal column keys
  stay stable so downstream parsers do not break when the UI language changes.

## i18n

English and Thai, `i18next`, choice persisted in `localStorage`. Thai renders with
Latin digits (`th-TH-u-nu-latn`) — Thai numerals in a metrics table are unreadable to
ops — and gets IBM Plex Sans Thai with a taller line-height so vowel and tone marks
are not clipped. `npm run check:i18n` fails the build on key drift between locales.

## Security

- Binds `127.0.0.1` by default. An off-loopback bind **refuses to start** without
  `DASHBOARD_TOKEN`: this service spawns load generators, so an open bind is a
  remote-execution surface.
- No user input is ever interpolated into a shell, a k6 script, or a YAML document.
  Children are spawned with an argv array and `shell: false`; the k6 config travels as
  an inert JSON file the script reads at init; the Artillery script is produced with
  `yaml.stringify`.
- Credentials are redacted out of the run's config snapshot before it is stored, and
  the snapshot is what the UI and exports read.
- Child processes are killed on stop and on SIGINT/SIGTERM — orphaned k6 processes
  would keep hammering the target.
- Concurrent runs are capped at 1 by default; parallel runs skew each other's numbers.
  Selecting several profiles queues them and runs them one after another rather
  than raising that cap.
- The Kafka monitor's SASL password is held in memory for the running monitor
  only. It is never returned by the API (status reports `hasPassword` instead),
  never written to the database, and never logged.

## Layout

```
src/
  shared/types.ts       normalized metric contract (shared with the UI)
  metrics/              histogram, 1s aggregator, threshold DSL
  runners/              k6 / artillery / kafka + run manager
  kafka/monitor.ts      consumer-lag monitor and per-run lag sampler
  export/csv.ts         CSV report generation
  api/                  routes + zod validation
web/src/
  features/             run, test profiles, history, kafka monitor, settings
  components/           panel, stat, uPlot time series, form fields
  i18n/                 en + th locales
```
