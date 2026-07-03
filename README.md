# log-processor-go

**log-processor-go** is a Kafka consumer that reads log messages from configured Apache Kafka topics, writes them to output files, and sends alert notifications via Telegram with optional pattern-based deduplication.

---

## Requirements

| Requirement | Version |
|---|---|
| Go | 1.21+ |
| Apache Kafka | 2.0+ (reachable broker) |

No database. No JVM.

---

## Build

```bash
go build -o log-processor-go .
```

Cross-compile for Linux from any machine:

```bash
GOOS=linux GOARCH=amd64 go build -o log-processor-go .
```

## Test

```bash
go test ./...
```

The suite needs no Kafka broker and no network. It covers the normalizer fingerprints (every token category and restrict mode), alert detection and dedup, the output-file failure scenarios (missing, deleted, truncated, recreated — never auto-created), pattern-file deletion/recreation self-healing, Telegram outage → auto-recovery and rate-limit handling (against a local fake server), the per-partition consume loop (via fakes), health endpoints, the status page, and config loading/validation. Run it before every deploy.

---

## Deploy

Copy the binary and config to the server:

```bash
scp log-processor-go user@server:/opt/log-processor-go/
scp config.example.json user@server:/etc/log-processor-go/config.json
```

> The filename matters: the systemd unit starts the app with `--config=/etc/log-processor-go/config.json`.

Install and start the systemd service:

```bash
sudo cp deploy/log-processor-go.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now log-processor-go
```

> Re-deploying after changes: copy the new binary and unit file, then `sudo systemctl daemon-reload && sudo systemctl restart log-processor-go`.

---

## Before First Run

The application **never creates any files** (no file is ever auto-created). Create the files you configure before starting.

- **Output files are required.** Every topic's `output` file must exist, be a regular file, and be writable, or the app aborts at startup with a clear error.
- **Pattern store files are optional at runtime.** If a configured `patternStoreFile` is missing or unwritable, the app still starts and keeps running — it just **disables deduplication** for that topic and sends every matching alert. Dedup **re-enables itself automatically** (checked every ~30 s) once the file becomes available. The app never creates the pattern file.

```bash
touch /data/logs/received_app1.log

# Only if patternStoreFile is configured (optional — if missing, dedup is simply disabled):
touch /data/patterns/app1-patterns.txt
```

---

## Run

The config file path is resolved in this order:

1. **CLI argument:**

   ```bash
   ./log-processor-go --config=/etc/log-processor-go/consumer_config.json
   ```

2. **Environment variable:**

   ```bash
   CONSUMER_CONFIG=/etc/log-processor-go/consumer_config.json ./log-processor-go
   ```

3. **Default fallback:** `config/consumer_config.json` (relative to working directory)

---

## Configuration

The application is driven by a single JSON file.

### Example: `consumer_config.json`

```json
{
  "bootstrapServers": "",

  "telegramBotToken": "",
  "telegramChatId": "",

  "topics": [
    {
      "topic": "app1-topic",
      "output": "/data/logs/received_app1.log",
      "alertKeywords": [
        "FATAL",
        "PANIC",
        "deadlock",
        "terminating connection",
        "could not"
      ],
      "patternStoreFile": "",
      "patternExtractRestrictMode": "high"
    }
  ]
}
```

### Field Reference

#### Top-level

| Field | Type | Required | Description |
|---|---|---|---|
| `bootstrapServers` | string | Yes | Kafka broker address(es), e.g. `host:9092` |
| `telegramBotToken` | string | Yes | Telegram Bot API token |
| `telegramChatId` | string | Yes | Telegram chat or channel ID to send alerts to |
| `topics` | array | Yes | One entry per Kafka topic to consume |

#### `topics[]`

| Field | Required | Description |
|---|---|---|
| `topic` | Yes | Kafka topic name |
| `output` | Yes | Absolute path to the output file — must exist before the app starts |
| `alertKeywords` | No | Keywords that trigger an alert (case-insensitive substring match). Omit or leave empty to disable alerting for this topic |
| `patternStoreFile` | No | Absolute path to the pattern store file for Telegram alert deduplication. Omit to send every matching alert without dedup. If set but missing or unwritable, dedup is disabled at runtime (every alert sent) and re-enables itself once the file is available again — the app never creates this file |
| `customNormalizationRules` | No | Array of `{"pattern", "replacement"}` regex rules applied **before** the built-in normalizer. Use to collapse app-specific tokens, e.g. `[{"pattern":"worker-\\d+","replacement":"<WORKER>"}]` |
| `patternExtractRestrictMode` | No | `high` (default), `medium`, or `low`. Controls how aggressively the normalizer collapses tokens before fingerprinting. Unknown values fall back to `high`. See [Pattern-extract restrict modes](#pattern-extract-restrict-modes) |

---

## Log Flow

For each assigned Kafka partition, a dedicated goroutine processes records in this order:

1. **Parse** the Kafka message JSON into a `LogEvent`
2. **Buffer** the `message` field into the batch string
3. Once the batch fills (size-capped by message count and bytes) or no more messages are immediately available — **flush** the batch to the output file (append-only, open/close per flush)
4. Only after the flush succeeds — **run alert detection** on each buffered event
5. **Enqueue** any alerts for Telegram delivery, then release the batch buffers

**Offsets are never committed.** The consumer joins with a group ID that is unique per process start, so on both the initial connect and every reconnect it begins at the **newest** offset. Any backlog that piled up while Kafka was unreachable is **skipped, not replayed** — this is what keeps memory flat through an outage and its recovery.

If the output flush fails, the batch is retried every second and consumption **pauses** until the file is writable again (no message already buffered is dropped). The failure is logged once, then only occasionally, and once more on recovery. No alert or Telegram work runs until the flush succeeds.

Each message is a JSON object with these fields:

| Field | Description |
|---|---|
| `serverName` | Originating server name |
| `path` | Log file path on the source server |
| `topic` | Kafka topic the message came from |
| `timestamp` | ISO-8601 UTC timestamp of the log event |
| `message` | The raw log line written to the output file |

---

## Alert Flow

When `alertKeywords` is configured, each message's `message` field is checked for a case-insensitive substring match after the output flush succeeds.

If a match is found and no `patternStoreFile` is configured → alert is queued for Telegram immediately.

If a match is found, a `patternStoreFile` is configured, and dedup is active:

1. The message is normalised into a structural pattern (variable tokens replaced with placeholders)
2. The pattern is checked against the in-memory set loaded from `patternStoreFile`
3. **Already known** → suppressed (DEBUG log line, no Telegram)
4. **New pattern** → appended to `patternStoreFile`, added to in-memory set, then Telegram queued

If the pattern file cannot be written (missing or unwritable), deduplication is **disabled** and **every** matching alert is sent — logged once. A background check (every ~30 s) **re-enables dedup automatically** as soon as the file is readable and writable again, reloading the pattern set from it. The app never recreates the file itself. (Dedup is treated as best-effort: when in doubt it sends rather than silently suppresses.)

---

## Alert Deduplication

### How normalisation works

Variable tokens in log messages are replaced with structural placeholders before fingerprinting:

| Category | Example input | Placeholder |
|---|---|---|
| Timestamp (ISO, Apache CLF, syslog, bare date or time; bracketed forms keep their brackets: `[<TS>]`) | `2026-05-19T10:23:45.123Z`, `19/May/2026:10:23:45 +0000`, `10:23:45` | `<TS>` |
| URL | `https://api.example.com/v1?x=1` | `<URL>` |
| Email | `alice@example.com` | `<EMAIL>` |
| Stack frame | `(Service.java:142)` | `(<FILE>:<LINE>)` |
| UUID | `550e8400-e29b-41d4-a716-446655440000` | `<UUID>` |
| MAC address | `aa:bb:cc:dd:ee:ff` | `<MAC>` |
| Hex (`0x…` or bare 8+ chars with both digits and letters) | `0xdeadbeef`, `cafebabe1234` | `<HEX>` |
| IPv4 with port | `192.168.1.5:5432` | `<IP>:<PORT>` |
| IPv4 | `192.168.1.5` | `<IP>` |
| IPv6 | `fe80::1ff:fe23:4567:890a` | `<IP6>` |
| Filesystem path (Unix or Windows) | `/var/log/app.log`, `C:\Program Files\foo` | `<PATH>` |
| Size with unit | `45GB`, `2048MiB` | `<SIZE>` |
| Duration with unit | `30s`, `1500ms` | `<DUR>` |
| Percent | `87%` | `<PCT>` |
| Bare number | `9876` | `<N>` |
| Quoted string | `"primary database"` | `<STR>` |
| Balanced JSON object | `{"a":1,"b":[2,3]}` | `<JSON>` |
| Bracketed array | `[1, 2, 3]` | `<ARR>` |
| `key=value` (plain-text value) | `host=db-server` | `host=<VAL>` |
| `key=value` (value contains a placeholder) | `pid=<N>` | `pid=<N>` |

Non-placeholder text is lowercased; placeholders keep their `<UPPERCASE>` form. Example:

```
Input:   could not connect to 192.168.1.5:5432 after 30s retries=3
Pattern: could not connect to <IP>:<PORT> after <DUR> retries=<N>
```

### Pattern-extract restrict modes

| Token shape | `high` (default) | `medium` | `low` |
|---|---|---|---|
| Identifier-embedded number (`worker7`, `req_123`) | kept literal | `worker<N>`, `req_<N>` | same as `medium` |
| All-letter hex word ≥ 8 chars (`deadbeef`, `cafebabe`) | kept literal | `<HEX>` | `<HEX>` |
| Short hex 4–7 chars with digit+letter mix (`0a3f`) | kept literal | `<HEX>` | `<HEX>` |
| Arbitrary alphabetic word ≥ 3 chars (not an alert keyword) | kept literal | kept literal | `<TOK>` |
| All other token types (`<TS>`, `<IP>`, `<UUID>`, `key=val`, …) | replaced | replaced | replaced |

Use `high` (the default) unless a topic produces repeated near-duplicate alerts that differ only in embedded numbers or hex strings. `low` risks merging genuinely different alerts under one fingerprint, permanently suppressing their Telegram notifications — verify the result per topic before rolling it out.

### Pattern store limits

The in-memory pattern set is hard-capped at **50,000 patterns**, and a single pattern longer than **8 KB** is never stored. Beyond either limit, dedup keeps working for the patterns already known, and new shapes simply always alert (fail open — nothing is ever silently suppressed by the limits). A warning is logged once when the set passes 10,000 patterns: that usually means normalization is missing a variable token type and the fix is a `customNormalizationRules` entry or a lower restrict mode, then clearing the pattern file.

### Resetting suppressed patterns

The pattern store file is watched at runtime via `fsnotify`. **Clearing or editing it in place** causes the in-memory pattern set to reload — **no app restart needed**.

```bash
# Clear all suppressed patterns (keep the file) — the running app picks this up within seconds
> /data/patterns/app1-patterns.txt

# Or selectively remove specific patterns by editing the file
```

**Deleting** the file (rather than clearing it in place) is different: the app does not recreate it, so the next attempt to persist a pattern fails and **deduplication disables** (every alert is then sent). **Recreate the file and dedup re-enables itself within ~30 seconds — no restart needed.** If the parent directory is removed, the app warns and waits for it to return — **no time limit** — then re-attaches the watcher and resumes automatically. The same applies to any watcher failure: it restarts itself with a backoff instead of giving up.

---

## Telegram Notifications

- Reachability is **probed at startup**; if Telegram is unreachable then, alert sending is disabled and a background probe (every ~60 s) re-enables it once Telegram answers
- Sends are serialised through a single goroutine — no parallel HTTP calls to the Telegram API
- Rate-limited to a minimum of 3 seconds between sends; on HTTP `429` the interval doubles (up to 60 s cap) and resets to 3 s on the next success
- On a **connectivity failure** (timeout / refused) that persists past retries, Telegram is treated as down and alert sending is **disabled** — logged once with an occasional reminder. A background probe (every ~60 s) **re-enables sending automatically** when Telegram is reachable again; alerts arriving while disabled are dropped, not queued
- **Persistent 429 rate-limiting** drops the individual alert but keeps alerting enabled (Telegram is reachable, just throttled)
- A bounded queue (1000 events) decouples the consumer goroutines from the sender. If it fills — e.g. Telegram is slow — excess alerts are dropped (logged). This is non-blocking: **a slow or full Telegram never backpressures or stalls log writing**
- Telegram failures never roll back or delay the output file write or the stored pattern

---

## Runtime Behavior

- A single consumer group ID, **unique per process start**, shared across topics; one goroutine per assigned partition
- **No offset commits** — the consumer always starts from the newest offset on connect and reconnect, so a Kafka outage's backlog is dropped rather than replayed (keeps memory flat through recovery)
- Per-flush batches are **size-capped** (by message count and bytes) and buffers are **released after each flush**, so a burst cannot grow memory without bound
- Invalid JSON records are logged with topic, partition, and offset, then skipped — a bad record does not stall the topic
- Output file is opened and closed on every flush — copytruncate-style log rotation (`> file`, `truncate -s 0`) can safely clear the output file at any time. Rename-style rotation is **not** supported (the app never recreates a renamed-away file)
- **Health endpoints on `:8080`:** `/livez` returns 200 whenever the process is running (liveness); `/healthz` returns 200 only when every topic's consumer is connected to Kafka (readiness); `/statusz` is a human-readable status page (see below)
- Logs are written to stdout/stderr (captured by systemd journal under the `log-processor` identifier)
- Graceful shutdown on `SIGTERM` or `SIGINT`: consumers stop, pending Telegram alerts drain (up to 30 s), then the process exits

### Status page

`GET :8080/statusz` shows every component's live state — including the self-healing degradations that would otherwise only be visible in old log lines. One glance answers "is anything currently off, since when, and what do I do about it":

```
log-processor-go — DEGRADED (2 of 4 components) — uptime 3h12m4s

OK        telegram                alert sending active
DEGRADED  kafka[app1-topic]       disconnected — reconnecting with backoff  [since 2026-07-03 10:02:11, 14m3s]
OK        output[app1-topic]      writing — last write 12s ago
DEGRADED  dedup[app1-topic]       OFF — open failed: no such file — every alert sent; recreate the pattern file to re-enable  [since 2026-07-03 08:14:05, 2h2m]
```

Per topic it reports the Kafka consumer (connected / reconnecting), output writing (writing / paused with the error), and dedup (active with pattern count / off with the reason / not configured), plus the shared Telegram sender. The first line says `OK` or `DEGRADED (n of m components)`.

### Resource safety

Memory is bounded by design, regardless of how the binary is launched (the limits below live in the code, not just the systemd unit):

- **Kafka outage** → idle, no messages flow, RAM flat. On recovery the backlog is skipped, so there is no catch-up surge.
- **Slow Kafka** → consumes at the broker's pace; the backlog waits on the broker, not in the app. RAM flat.
- **Slow / full Telegram** → the 1000-event alert queue drops on overflow (never blocks); log writing is unaffected.
- **Per-process memory** → bounded by sarama's fetch buffers plus the size-capped batch per partition, and the in-memory dedup set is hard-capped (see below). The binary applies a **built-in `GOMEMLIMIT` of 112 MiB** when none is set in the environment, so the GC discipline holds even on a bare run; the systemd unit sets the same value explicitly plus a `MemoryMax=128M` cgroup hard cap. Running the bare binary keeps every code-level bound — it only loses the kernel-enforced cap and auto-restart.
- **Bug containment** → a panic while processing one partition is recovered and logged; that consumer reconnects and every other topic keeps running. The process does not die.

### Failure policy

| Failure | Behavior |
|---|---|
| Output file missing at startup | Startup fails with a clear error message |
| Pattern store file missing at startup | Starts anyway; dedup disabled for that topic (every alert sent) until the file appears, then re-enables automatically |
| Kafka unreachable at startup | Retries with backoff until reachable; `/healthz` reports not-ready; no RAM growth |
| Kafka drops while running | Reconnects, then resumes from the newest offset (outage backlog dropped) |
| Output file append fails while running | Batch retried every second, consumption pauses; logged once + ~30 s reminder + recovery log; no buffered message lost; file never recreated |
| Output file deleted while running | Same retry/pause loop; holds until you recreate the file (not auto-created) |
| Output file truncated/cleared while running | Next flush writes to the start of the cleared file — transparent |
| Pattern file unwritable or deleted while running | Dedup disabled, every alert sent; re-enables automatically once the file is back (file never auto-created) |
| Telegram unreachable (startup or while running) | Alert sending disabled; logged once + occasional reminder; re-enables automatically when Telegram answers again |
| Telegram slow, queue full, or persistently rate-limited | Excess alerts dropped (logged); log writing unaffected |
| Bug/panic while processing a partition | Recovered and logged; that consumer reconnects; the process and all other topics keep running |
| Pattern set hits the 50,000 cap | Known patterns keep deduping; new shapes always alert; logged once |

---

## File Safety

The app only ever touches the files you configure. Full audit:

| Operation | Paths touched |
|---|---|
| Read | Config JSON, pattern store files |
| Append (existing file only) | Output log files, pattern store files |
| Watch (inotify, read-only) | Parent directory of each pattern store file |
| Outbound TCP | Kafka `bootstrapServers`, `api.telegram.org` |
| Inbound TCP | Health server on `:8080` (`/livez`, `/healthz`, `/statusz`) |

The app **never creates any file** (no `O_CREATE` anywhere). No temp files, no writes to `/tmp`, no home directory changes, no subprocesses spawned. The only inbound port is the health server on `:8080`.

---

## Project Layout

```
log-processor-go/
├── main.go               entry point — wiring, path validation, built-in memory
│                         limit, /statusz assembly, signal handling, shutdown
├── go.mod
├── config/
│   ├── config.go         AppConfig, TopicConfig, NormalizationRule structs
│   └── loader.go         JSON config loading + path resolution + validation
├── logpkg/
│   ├── event.go          LogEvent
│   ├── detector.go       AlertDetector — case-insensitive keyword match
│   ├── restrict_mode.go  HIGH / MEDIUM / LOW enum
│   ├── normalizer.go     LogMessageNormalizer — 22-rule regex pipeline
│   └── pattern_store.go  AlertPatternStore — file-backed dedup set with size caps,
│                         fsnotify watcher, and self-healing recovery
├── pipeline/
│   ├── handler.go        LogHandler — two-phase record processing (stateless, goroutine-safe)
│   └── batch_writer.go   BatchFileWriter — mutex-serialised append-only file writer
├── health/
│   └── health.go         ReadinessTracker (/healthz), /livez, /statusz status page
├── kafka/
│   ├── poll_loop.go      PollLoop (sarama ConsumerGroupHandler) + sarama config
│   └── retry.go          consumer-group creation with exponential-backoff retry
├── notification/
│   ├── notifier.go       Notifier interface
│   ├── formatter.go      TelegramAlertFormatter — LogEvent → alert text
│   └── telegram.go       TelegramNotificationService — probe, rate-limit,
│                         auto-disable + background auto-recovery
├── tools/
│   └── producer/         developer-only Kafka message generator (not deployed)
└── deploy/
    └── log-processor-go.service   systemd unit file
```

Every package also has `*_test.go` files — the automated suite described under [Test](#test).

---

## Notes

- Do not commit real credentials in the config file — use `config.example.json` as a blank template
- Kafka topics must exist before the application starts (or broker auto-creation must be enabled)
- The application exposes a health server on `:8080` (`/livez`, `/healthz`, `/statusz`) and no other inbound ports
