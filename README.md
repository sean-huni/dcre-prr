# dcre-prr

Payments Request Reader: boundary stage that ingests OnHost ENDO copybook files into the **payments** spine (`tx_header` / `tx_entry`) in `dcre_pay`.

## What it does

PRR is the first stage of the ENDO payments request DAG. OnHost drops a fixed-width copybook file into the per-client exchange (`onhost-req-endo/in`), AGT registers the arrival and launches PRR as a short-lived Kubernetes Job with `arrival.id` as the identifying JobParameter (R-16). PRR parses the header, runs the file-fatal structural tier (R-19), then ingests every detail record; it is the single writer of the payments spine (R-04), and every downstream stage transitions via the database, never via files (R-30).

### Why this repo exists: the payments split

PRR was forked from [dcre-crr](https://github.com/sean-huni/dcre-crr) and then REDUCED, rather than CRR being renamed, so CRR keeps its history and its repo (the maf-to-mas precedent in this estate).

Before the split, one reader and one database served both families: AGT passed a `flow` launch argument, CRR stamped `tx_header.flow` as `COL` or `PAY`, and downstream arms selected on that column (SCRUM-69). **PRR carries no flow discriminator at all.** The database is the discriminator now: a row in `dcre_pay.tx_header` IS a payment, so `tx_header` here has no `flow` column, `HeaderService` takes no `flow` parameter, and there is no `COL` branch to take.

Two tests enforce that, and both have been seen red:

- `PayFlowOnlyTest` walks `src/main/java` and fails if any source carries `FLOW_PAY`, `validatedFlow` or the literal `"COL"`.
- The BDD scenario *"The payments spine has no flow column to discriminate on"* asserts against `information_schema`, with a control assertion on `arrival_id` so a zero cannot be a mistyped table name.

The copybook LAYOUT is deliberately unchanged. ENDO payment books and DC collection books are the same OnHost copybook (109-content header, V1 161 / V2 169 / V3 204 details), which is exactly why a launch argument could once route an ENDO arrival into CRR. What the split changed is where the rows land, not how the bytes are cut. See `PaymentRecords`.

## Architecture and principles

Ephemeral Spring Boot 4.1.0 / Spring Batch 6 / Java 25 batch job, not a server: `ExitCodeMain` wires the Batch outcome into the JVM exit code (R-34) and the job runner fires `prrJob` once per launch. CockroachDB is reached through the PostgreSQL driver.

SOLID as applied here:

- Thin entry adapters: `HeaderTasklet` extracts JobParameters and calls one `HeaderService` method; business logic lives in the service tier, persistence only via `data/repo`.
- One responsibility per unit: `PaymentRecords` (the file's record shapes, in one place), `LineRangePartitioner` (byte-offset ranges), `FixedRecordRangeReader` (ISO_8859_1, byte-transparent range read), `SpineWriter` (record parse + content hash), `TxEntryBatchDao` (sole owner of the `tx_entry` write SQL).
- Layer-first packages: `batch/`, `config/`, `data/model/`, `data/repo/`, `service/`.

12FactorApp Alignment - https://12factor.net/ : config strictly from the environment over committed working dev defaults (a clean clone boots with no `.env`), stateless one-shot process, CockroachDB and the exchange directory as attached backing services, dev/prod parity via Testcontainers CockroachDB in the test suite.

### Job structure (`za.co.fnb.dcre.prr.config.PrrJobConfig`)

1. `headerStep` (tasklet): `HeaderTasklet -> HeaderService` parses record 0 against `Layouts.HEADER` (attested content length 109) and runs the structural tier: empty file, header length, layout_version with V1 fail-closed unless `dcre.v1-enabled` (A-2), declared tx_count vs actual records, R-31 filename-client vs header destination_id. On success it upserts `tx_header` keyed on arrival_id. On failure the step exits `FILE_FATAL`, the reason lands in the execution context as `fileFatalReason`, and the job transitions straight to end COMPLETED: a business verdict, never a process death. The step carries the shared `CrdbRetryExceptionHandler` so commit-time CRDB 40001 serialization aborts re-run the tasklet instead of failing the job.
2. `detailStep` (R-41 partitioned manager/worker pair): `LineRangePartitioner` splits the detail records into contiguous byte-offset ranges (all probed as byte counts so non-UTF-8 bytes never desync the offsets; a ragged length or an LRECL matching no layout is FILE_FATAL). Grid size is `PartitionSizer.partitions(dcre.prr.max-partitions)` (cgroup-aware CPU clamp) and the ranges fan out onto a `VirtualThreadTaskExecutor`. Each worker (`detailWorkerStep`, chunk 100) reads its range with `FixedRecordRangeReader` (restart resumes mid-range via `read.count`, R-05) into `SpineWriter`, which picks the layout by LRECL (`DETAIL_V3` = 204, `DETAIL_V2` = 169, or `DETAIL_V1` = 161 which fails closed unless V1 is enabled), parses amounts through `MoneyText` at `dcre.amount-scale`, computes a SHA-256 `content_hash` over the essential business fields, and derives `sequence` from the record's file position (`recordIndex + 1`, never a shared counter, so partitioned ingest is deterministic). The 40001 retry is deliberately NOT registered on the worker step: a swallowed commit-time abort there would silently drop the in-flight chunk (verified empirically 2026-07-14); a worker commit abort stays step-FAILED then relaunch, which resumes idempotently.

The record layout is chosen PER RECORD through `PaymentRecords` (a `LayoutResolver` from `platform-copybook`), which takes the record INDEX as well as the line. The index is load-bearing: a generator pads the header out to the detail LRECL while a real file does not, so a padded 169-byte header is length-identical to a V2 detail and only its position at index 0 tells them apart. A length-only resolver would cut record 0 of every generated file with the detail table.

### Idempotent restart semantics

- Identifying JobParameter `arrival.id` (R-16): rerunning the same identity refuses with JobInstanceAlreadyComplete and the spine stays unchanged.
- Writes go through `TxEntryBatchDao`'s guarded `INSERT ... ON CONFLICT (arrival_id, sequence) DO UPDATE`, batched 500 rows per `JdbcTemplate.batchUpdate` (CRDB `UPSERT` arbitrates on the PK only, hence the explicit business-key conflict target).
- `BatchMetaConfig` runs `StaleExecutionSweeper.abandonStale(ds, "PRR_BATCH_", 60)` before the job runner fires (A-39a): a relaunch after a pod kill never throws JobExecutionAlreadyRunning.

### Outcome seam and failure evidence

The shared `OutcomeSeamListener` writes the business-verdict seam file (SYNTHETIC-CONTRACT, R-35) to `<exchange-root>/outcomes/<JOB_NAME>` (fallback `local-prr-<executionId>`): `BUSINESS_ACCEPTED` on a clean run, `BUSINESS_FILE_FATAL` when `fileFatalReason` is set. A non-COMPLETED execution writes nothing: the exit code and the K8s Failed condition are the witnesses, and AGT treats absence as never-success (R-33 arbiter clause). The `PRR_BATCH_` metadata is the step-grain diagnostics annex only; AGT never reads it for orchestration decisions.

### Database and batch metadata

PRR owns `dcre_pay` outright. Liquibase owns the schema, with per-service history tables (`prr_databasechangelog` / `prr_databasechangeloglock`):

- `2026/08/001-pay-spine.xml`: `tx_header` (UNIQUE arrival_id; raw + canonical msg_id per R-15; **no flow column**) and `tx_entry` (UNIQUE arrival_id/sequence; amount_raw kept alongside the config-scaled DECIMAL while A-1 is open; `mandate_ref` for the M10 payment-to-mandate link; `content_hash` plus the covering index `(arrival_id, content_hash, sequence)`). Pure typed XML, BaseEntity columns declared in the `createTable` rather than bolted on by a later ALTER, so this is one changeset where CRR needed four.
- `2026/08/002-batch-metadata.xml`: Liquibase-owned copy of the Spring Batch 6.0.4 postgres DDL (via `sqlFile`, vendored as `batch-metadata-prr.sql`), prefixed `PRR_BATCH_`, EXIT_MESSAGE widened to TEXT so CRDB-driver cause chains are never truncated (A-39b).

Every changeset is guarded `<preConditions onFail="CONTINUE">`, never `MARK_RAN`. MARK_RAN records the skip permanently, so a database that was merely not-yet-ready at the moment of the check never gets the change at all; CONTINUE leaves the changeset unlogged and re-evaluated on the next run, which is what a bootstrap guard actually wants.

Key rules: R-04 single writer, R-05 restart-without-duplication, R-15 raw + canonical identity, R-16 launch identity, R-19 file-fatal tier, R-30 boundary file I/O, R-31 filename grammar cross-check, R-33 two-plane failure evidence, R-34 exit-code wiring + prefixed metadata, R-35 synthetic seam contract, R-41 intra-file parallelism + content hash, A-2 V1 fail-closed.

## Prerequisites

- Java 25 (`.sdkmanrc` pins `25-tem`)
- Docker (Testcontainers tests and image build)
- Platform libs published to Maven Local (no remote repository): `za.co.fnb.dcre:platform-copybook:0.2.0`, `platform-persistence:0.1.0`, `platform-batch:0.1.0`. Run `./gradlew publishToMavenLocal` in each platform repo; publish chain `dcre-platform-model` then `dcre-platform-files` then `dcre-platform-batch`, with `platform-persistence` and `platform-copybook` standalone. This repo declares `platform-copybook` (`Layouts`, `LayoutResolver`, `CopybookReader`, `FixedWidthRecord`), `platform-batch` (`ExitCodeMain`, `OutcomeSeamListener`, `StaleExecutionSweeper`, `CrdbRetryExceptionHandler`, `PartitionSizer`, `HeartbeatWriter`; `R31Filename` / `MoneyText` / `OpaqueRef` arrive transitively) and `platform-persistence` (`BaseEntity`, `JdbcConfig`).
- A reachable CockroachDB for a real local run (committed default: `localhost:26257`, database `dcre_pay`); the dcre-infra kind cluster provides one.

## Quickstart

Clean clone, no `.env` needed (working dev defaults are committed in `application.yml`):

```bash
./gradlew build          # full suite, Docker required
./gradlew bootJar        # build/libs/prr-2.0.jar

# local one-shot run against a reachable CockroachDB
./gradlew bootRun --args="arrival.id=<uuid> \
  input.file=/path/to/file.txt \
  original.name=FNBRF01_DCRERF2026071112000002.txt"
```

## Configuration

Env over committed dev defaults (precedence: yml default < environment).

| Env | Default | Purpose |
|---|---|---|
| `DCRE_PAY_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_pay?sslmode=disable` | Payments DB (CockroachDB) |
| `DCRE_PAY_DB_USER` | `root` | DB user |
| `DCRE_PAY_DB_PASSWORD` | (empty) | DB password |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam file |
| `DCRE_AMOUNT_SCALE` | `2` | MoneyText scale (SYNTHETIC-CONTRACT while A-1 is open) |
| `DCRE_V1_ENABLED` | `false` | V1 layout gate (A-2: fails closed in production) |
| `DCRE_PRR_MAX_PARTITIONS` | `5` | Upper bound on the detailStep partition grid (R-41); actual grid = clamp(available CPUs, 1, this) |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | Heartbeat target (`agt_ops.launch_intent`) |
| `JOB_NAME` | (unset: heartbeat disabled, seam falls back to `local-prr-<executionId>`) | Set by AGT on the K8s Job |

## Testing

```bash
./gradlew clean build
```

38 tests, Docker required: integration tests run on Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3`.

- `PayFlowOnlyTest`: no flow discriminator survives anywhere in `src/main/java`.
- `PrrJobTest`: the V2 sample parses into 1 header + 30 entries with MoneyText scaling; the same identity refuses a second run without duplicating (R-05/R-16); a V1 file completes as FILE_FATAL with zero details persisted; an unpadded-header file and a non-UTF-8 byte both ingest byte-exactly; a V3 book carries `mandate_ref` while a V2 book leaves it NULL; the outcome seam is byte-exact for both verdicts.
- `PrrPartitionDeterminismTest`: the same fixture under `dcre.prr.max-partitions` 1 vs 5 in two `@Nested` contexts yields identical `(sequence, e2e, content_hash)` rows (R-41 determinism).
- `PrrJobConfigRetryTest`: headerStep re-runs the tasklet on commit-time CRDB 40001 serialization aborts.
- `LineRangePartitionerTest` / `FixedRecordRangeReaderTest`: byte-offset ranges, missing-final-newline, ragged-length and wrong-separator fail-closed, mid-range restart.
- `SpineWriterTest`: content hash covers the essential business fields only; V3 `mandate_ref` mapping.
- Cucumber BDD suite (`CucumberSuiteTest`, `features/prr_boundary_reader.feature`, tag `@prr`): business-language scenarios over the real job + CockroachDB, including the no-flow-column schema invariant.

Fixtures: `src/test/resources/dcre_copybook_v{1,2,3}_*.txt`. They keep their `_dc_` filenames because they are the byte-identical OnHost copybook both families use; renaming them would imply a payments-specific layout that does not exist.

## Local cluster deployment

```bash
./gradlew bootJar
docker build --platform linux/amd64 -t dcre-prr:2.1.1 .
kind load docker-image --name dcre-dev dcre-prr:2.1.1
```

Image base: `eclipse-temurin:25-jre-alpine`. Image tags follow the fleet release tags (digits-only SemVer); the Gradle project version inside the jar name stays `2.0`, matching the rest of the fleet.

AGT launches PRR as an ephemeral K8s Job per registered ENDO arrival: the JobParameters arrive as program args and `JOB_NAME` is set in the Job env. **The AGT wiring and the emission-gate flip are NOT part of this phase**; PRR is buildable and verifiable standalone, and AGT still routes ENDO arrivals to CRR until that work lands.

## Related repositories

- Orchestrator: [dcre-agt](https://github.com/sean-huni/dcre-agt)
- Collections request DAG: [dcre-crr](https://github.com/sean-huni/dcre-crr), [dcre-ctv](https://github.com/sean-huni/dcre-ctv), [dcre-cde](https://github.com/sean-huni/dcre-cde), [dcre-cir](https://github.com/sean-huni/dcre-cir), [dcre-ais](https://github.com/sean-huni/dcre-ais)
- Platform libs: [dcre-platform-model](https://github.com/sean-huni/dcre-platform-model), [dcre-platform-files](https://github.com/sean-huni/dcre-platform-files), [dcre-platform-batch](https://github.com/sean-huni/dcre-platform-batch), [dcre-platform-persistence](https://github.com/sean-huni/dcre-platform-persistence)
- Support: [dcre-infra](https://github.com/sean-huni/dcre-infra), [dcre-fixture-toolkit](https://github.com/sean-huni/dcre-fixture-toolkit), [dcre-design-register](https://github.com/sean-huni/dcre-design-register)
