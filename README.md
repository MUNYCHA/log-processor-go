# log-processor-go

**log-processor-go** is a Kafka consumer that reads log messages from configured Apache Kafka topics, writes them to output files, and sends alert notifications via Telegram with optional pattern-based deduplication.

---

## Requirements

| Requirement | Version |
|---|---|
| Go | 1.21+ |
| Apache Kafka | Reachable broker |

No database. No JVM.

---

## Build

```bash
go build -o log-processor .
```

Cross-compile for Linux from any machine:

```bash
GOOS=linux GOARCH=amd64 go build -o log-processor .
```

---

## Deploy

Copy the binary and config to the server:

```bash
scp log-processor user@server:/opt/log-processor-go/
scp config.example.json user@server:/etc/log-processor-go/consumer_config.json
```

Install and start the systemd service:

```bash
sudo cp deploy/log-processor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now log-processor
```

---

## Before First Run

The application **never creates any files**. Every output file and pattern store file declared in the config must exist and be writable before the app starts. The app aborts at startup with a clear error if any configured path is missing, is not a regular file, or is not writable.

Create the required files manually before starting:

```bash
touch /data/logs/received_app1.log

# Only if patternStoreFile is configured for a topic:
touch /data/patterns/app1-patterns.txt
```

---

## Run

The config file path is resolved in this order:

1. **CLI argument:**

   ```bash
   ./log-processor --config=/etc/log-processor-go/consumer_config.json
   ```

2. **Environment variable:**

   ```bash
   CONSUMER_CONFIG=/etc/log-processor-go/consumer_config.json ./log-processor
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
| `patternStoreFile` | No | Absolute path to the pattern store file for Telegram alert deduplication — must exist before the app starts. Omit to send every matching alert without dedup |
| `customNormalizationRules` | No | Array of `{"pattern", "replacement"}` regex rules applied **before** the built-in normalizer. Use to collapse app-specific tokens, e.g. `[{"pattern":"worker-\\d+","replacement":"<WORKER>"}]` |
| `patternExtractRestrictMode` | No | `high` (default), `medium`, or `low`. Controls how aggressively the normalizer collapses tokens before fingerprinting. Unknown values fall back to `high`. See [Pattern-extract restrict modes](#pattern-extract-restrict-modes) |

---

## Log Flow

Each topic runs its own consumer group. For each assigned Kafka partition, a dedicated goroutine processes records in this order:

1. **Parse** the Kafka message JSON into a `LogEvent`
2. **Buffer** the `message` field into the batch string
3. After all available messages are batched — **flush** the batch to the output file (append-only, open/close per flush)
4. Only after the flush succeeds — **run alert detection** on each buffered event
5. **Commit** Kafka offsets
6. **Enqueue** any alerts for Telegram delivery

If the output flush fails, the batch is retried every second. Kafka offsets are not committed and messages are re-delivered when the session recovers. No alert or Telegram work runs until the flush succeeds.

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

If a match is found and a `patternStoreFile` is configured:

1. The message is normalised into a structural pattern (variable tokens replaced with placeholders)
2. The pattern is checked against the in-memory set loaded from `patternStoreFile`
3. **Already known** → suppressed (DEBUG log line, no Telegram)
4. **New pattern** → appended to `patternStoreFile`, added to in-memory set, then Telegram queued

A new pattern is written to disk before it is exposed in memory. If the disk write fails the alert is not sent — this avoids sending a Telegram notification for a pattern that will not be remembered, which would cause the same alert to fire again on the next occurrence.

---

## Alert Deduplication

### How normalisation works

Variable tokens in log messages are replaced with structural placeholders before fingerprinting:

| Category | Example input | Placeholder |
|---|---|---|
| Timestamp (ISO, Apache CLF, syslog, bare date or time, bracketed `[..]`) | `2026-05-19T10:23:45.123Z`, `19/May/2026:10:23:45 +0000`, `10:23:45` | `<TS>` |
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

### Resetting suppressed patterns

The pattern store file is watched at runtime via `fsnotify`. Editing or clearing it causes the in-memory pattern set to reload — **no app restart needed**.

```bash
# Clear all suppressed patterns — the running app picks this up within seconds
> /data/patterns/app1-patterns.txt

# Or selectively remove specific patterns by editing the file
```

If the file is deleted, the in-memory set is cleared and the app keeps running. The next new alert recreates the file automatically. If the parent directory is removed, the app warns and waits up to 2 minutes for it to return before the watcher gives up.

---

## Telegram Notifications

- Sends are serialised through a single goroutine — no parallel HTTP calls to the Telegram API
- Rate-limited to a minimum of 3 seconds between sends
- On HTTP `429` the send interval doubles (up to 60 s cap) and resets to 3 s on the next success
- Up to 3 retries per alert on timeout; after that the alert is logged and dropped
- A bounded queue (1000 events) decouples the consumer goroutines from the sender — if the queue fills, excess alerts are dropped and an error is logged
- Telegram failures never roll back the output file write, the Kafka commit, or the stored pattern

---

## Runtime Behavior

- One consumer group per configured topic; one goroutine per assigned partition within that topic
- Manual Kafka offset commit — offsets are committed only after the output file flush succeeds
- Invalid JSON records are logged with topic, partition, and offset, then skipped — a bad record does not stall the topic
- Output file is opened and closed on every flush — log-rotation tools (`> file`, `truncate -s 0`) can safely clear or replace the output file at any time without conflicting with the app
- Logs are written to stdout/stderr (captured by systemd journal under the `log-processor` identifier)
- Graceful shutdown on `SIGTERM` or `SIGINT`: consumers stop, in-flight Kafka commits complete, pending Telegram alerts drain (up to 30 s), then the process exits

### Failure policy

| Failure | Behavior |
|---|---|
| Output file or pattern store file missing at startup | Startup fails with a clear error message |
| Output file append fails while running | Topic batch retried every second; Kafka not committed; alert work not started |
| Output file deleted while running | Retry loop holds until file is recreated or shutdown |
| Output file truncated/cleared while running | Next flush writes to the start of the cleared file — transparent |
| Pattern file deleted while running | In-memory patterns cleared; file recreated on next new alert |
| New pattern cannot be written to disk | Error logged; pattern not remembered; Telegram not sent for that occurrence |
| Telegram request fails | Error logged after retries; output file write and Kafka commit are not rolled back |
| Telegram queue full | Error logged; excess alert dropped |

---

## File Safety

The app only ever touches the files you configure. Full audit:

| Operation | Paths touched |
|---|---|
| Read | Config JSON, pattern store files |
| Append (existing file) | Output log files, pattern store files |
| Create (only if previously deleted) | Pattern store files |
| Watch (inotify, read-only) | Parent directory of each pattern store file |
| Outbound TCP | Kafka `bootstrapServers`, `api.telegram.org` |

No temp files, no writes to `/tmp`, no home directory changes, no subprocesses spawned, no inbound network ports opened.

---

## Project Layout

```
log-processor-go/
├── main.go               entry point — wiring, path validation, signal handling, shutdown
├── go.mod
├── config/
│   ├── config.go         AppConfig, TopicConfig, NormalizationRule structs
│   └── loader.go         JSON config loading + path resolution + validation
├── logpkg/
│   ├── event.go          LogEvent
│   ├── detector.go       AlertDetector — case-insensitive keyword match
│   ├── restrict_mode.go  HIGH / MEDIUM / LOW enum
│   ├── normalizer.go     LogMessageNormalizer — 21-rule regex pipeline
│   └── pattern_store.go  AlertPatternStore — file-backed dedup set + fsnotify watcher
├── pipeline/
│   ├── handler.go        LogHandler — two-phase record processing (stateless, goroutine-safe)
│   └── batch_writer.go   BatchFileWriter — mutex-serialised append-only file writer
├── kafka/
│   └── poll_loop.go      PollLoop (sarama ConsumerGroupHandler) + sarama config
├── notification/
│   ├── notifier.go       Notifier interface
│   ├── formatter.go      TelegramAlertFormatter — LogEvent → alert text
│   └── telegram.go       TelegramNotificationService — rate-limited HTTP POST
└── deploy/
    └── log-processor.service   systemd unit file
```

---

## Notes

- Do not commit real credentials in the config file — use `config.example.json` as a blank template
- Kafka topics must exist before the application starts (or broker auto-creation must be enabled)
- The application does not expose any HTTP endpoints or inbound ports
