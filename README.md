# dcre-prr

Payments Request Reader: boundary stage that ingests OnHost ENDO copybook files into the **payments** spine (`tx_header` / `tx_entry`) in `dcre_pay`.

## What it does

PRR is the first stage of the ENDO payments request DAG:

```
ENDO payments  onhost-req-endo:  PRR -> PTV -> PAI -> { PRW -> Fintegrate request
                                                     || PIR -> OnHost response }
                                 (immediate: no CDE, no mandate gate; PIR is the responder,
                                  PRW and PIR are BOTH terminal, Emission.NONE)

DC collections onhost-req:       CRR -> CTV -> { CDE -> CRW -> Fintegrate request
                                                || CIR -> OnHost response }
                                 (CDE future-dates the work; CRW is the writer, CIR the responder)
```

PTV, **PRW** and PIR are the payments-side counterparts of CTV, **CRW** and CIR. **PAI is not a
counterpart of anything**: it is the former `ais` stage, renamed and retargeted at `dcre_pay` rather
than rebuilt, and it sits only in the ENDO route, never in the DC route. CDE is
the only collections stage with no payments counterpart, which is what "immediate" means. Both
writers are easy to drop and neither must be: CRW and PRW generate and write the Fintegrate request
to the directory, so a leg rendered without its writer reads as a responder and nothing else. Each
arm forks into a writer branch and a responder branch, payments after PAI and collections after CTV,
with CDE sitting between the collections fork and CRW. All four payments stages (PTV, PAI, PRW, PIR)
exist as their own repositories and ENDO arrivals run the payments DAG over `dcre_pay`.

OnHost drops a fixed-width copybook file into the per-client exchange (`onhost-req-endo/in`), AGT registers the arrival and launches PRR as a short-lived Kubernetes Job with `arrival.id` as the identifying JobParameter (R-16). PRR parses the header, runs the file-fatal structural tier (R-19), then ingests every detail record; it is the single writer of the payments spine (R-04), and every downstream stage transitions via the database, never via files (R-30).

### Why this repo exists: the payments split

PRR was forked from [dcre-crr](https://github.com/sean-huni/dcre-crr) and then REDUCED, rather than CRR being renamed, so CRR keeps its history and its repo (the maf-to-mas precedent in this estate).

Before the split, one reader and one database served both families: AGT passed a `flow` launch argument, CRR stamped `tx_header.flow` as `COL` or `PAY`, and downstream arms selected on that column (SCRUM-69). **PRR carries no flow discriminator at all.** The database is the discriminator now: a row in `dcre_pay.tx_header` IS a payment, so `tx_header` here has no `flow` column, `HeaderService` takes no `flow` parameter, and there is no `COL` branch to take.

Five assertions across two test classes enforce that, each seen red on its own. They deliberately attack the concept from five different directions, because a scan for string literals is satisfied by DELETING three literals rather than by removing the concept: in CRR the flow token appears in four main sources and those three literals live in only one of them, so a partial strip leaving the entity field, the upsert column or the job parameter would still have gone green.

| Assertion | What it closes |
|---|---|
| `PayFlowOnlyTest.prrCarriesNoFlowDiscriminator` | the constants and the validator, by name, in `src/main/java` |
| `PayFlowOnlyTest.spineHeaderDeclaresNoFlowField` | the PERSISTED shape, by reflection on `TxHeaderEntity`, so no comment rewording can hide it |
| `PayFlowOnlyTest.noResourceReintroducesAFlowColumnOrKey` | a Liquibase changeset or a yml key putting the column back, which the java-only walk cannot reach |
| `PayFlowOnlyTest.noSourceReadsAFlowJobParameter` | the LAUNCH surface, so a launcher cannot hand PRR a flow that is silently ignored |
| BDD *"The payments spine has no flow column to discriminate on"* | the live SCHEMA, via `information_schema`, with a control assertion on `arrival_id` |

The copybook LAYOUT is deliberately unchanged. ENDO payment books and DC collection books are the same OnHost copybook (109-content header, V1 161 / V2 169 / V3 204 details), which is exactly why a launch argument could once route an ENDO arrival into CRR. What the split changed is where the rows land, not how the bytes are cut.

The authority is the fixture toolkit: `generate_dcre_copybook.py` takes `--flow dc|endo` and documents that "Physical DC/ENDO distinction is provenance-level (MFT route), so it lives in the manifest only." A payments-specific layout table would have been a fabricated contract. See `PaymentRecords`, and `EndoDcIdentityTest`, which commits BOTH books and asserts they slice and persist identically, so the claim goes red the day OnHost genuinely diverges instead of quietly becoming false.

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

- `2026/08/001-pay-spine.xml`: `tx_header` (UNIQUE arrival_id; raw + canonical msg_id per R-15; **no flow column**) and `tx_entry` (UNIQUE arrival_id/sequence; amount_raw kept alongside the config-scaled DECIMAL while A-1 is open; `mandate_ref`, a V3 field of the shared book that ENDO does not populate (M10 is the COLLECTION-to-mandate link and the mandate gate is DC-only per R-19, so expect it NULL on every payment row); `content_hash` plus the covering index `(arrival_id, content_hash, sequence)`). Pure typed XML, BaseEntity columns declared in the `createTable` rather than bolted on by a later ALTER, so this one FILE (four changesets) replaces the four CRR changelogs that between them build the same schema: `001-spine`, `003-layering`, `004-content-hash`, `006-mandate-ref`.
- `2026/08/002-batch-metadata.xml`: Liquibase-owned copy of the Spring Batch 6.0.4 postgres DDL (via `sqlFile`, vendored as `batch-metadata-prr.sql`), prefixed `PRR_BATCH_`, EXIT_MESSAGE widened to TEXT so CRDB-driver cause chains are never truncated (A-39b).

No changeset in this repo uses `MARK_RAN`, but the two files reach re-run safety by different means and it is worth knowing which is which. The four spine changesets in `001` are guarded `<preConditions onFail="CONTINUE">`: MARK_RAN records the skip permanently, so a database that was merely not-yet-ready at the moment of the check never gets the change at all, while CONTINUE leaves the changeset unlogged and re-evaluated on the next run, which is what a bootstrap guard actually wants. `002` carries no precondition at all, deliberately: it is a single `sqlFile` creating nine objects, and a precondition sampling one of them reads a half-applied file as complete and skips the rest forever (A-81, which left services with tables and no sequences). Its idempotency lives in the DDL instead, `IF NOT EXISTS` on every CREATE, which converges from any partial state.

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
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_pay?sslmode=disable` | Payments DB (CockroachDB) |
| `DCRE_DB_USER` | `root` | DB user |
| `DCRE_DB_PASSWORD` | (empty) | DB password |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | Exchange root for the outcome seam file |
| `DCRE_AMOUNT_SCALE` | `2` | MoneyText scale (SYNTHETIC-CONTRACT while A-1 is open) |
| `DCRE_V1_ENABLED` | `false` | V1 layout gate (A-2: fails closed in production) |
| `DCRE_PRR_MAX_PARTITIONS` | `5` | Upper bound on the detailStep partition grid (R-41); actual grid = clamp(available CPUs, 1, this) |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | Heartbeat target (`agt_ops.launch_intent`) |
| `DCRE_AGTOPS_DB_USER` | `root` | Heartbeat DB user |
| `DCRE_AGTOPS_DB_PASSWORD` | (empty) | Heartbeat DB password |
| `JOB_NAME` | (unset: heartbeat disabled, seam falls back to `local-prr-<executionId>`) | Set by AGT on the K8s Job |

## Testing

```bash
./gradlew clean build
```

46 tests, Docker required: integration tests run on Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3`.

- `PayFlowOnlyTest` (4): the flow concept is gone from the sources, the entity's declared fields, the resources and the launch surface. See the table above.
- `PrrJobTest` (10): the DC V2 sample parses into 1 header + 30 entries with MoneyText scaling; the same identity refuses a second run without duplicating (R-05/R-16); a V1 file completes as FILE_FATAL with zero details persisted; an unpadded-header file and a non-UTF-8 byte both ingest byte-exactly; the V3 DC fixture carries `mandate_ref` while a V2 book leaves it NULL, which exercises the column mapping without contradicting the production expectation that ENDO rows never populate it; the outcome seam is byte-exact for both verdicts; and the ENDO book ingests to 1 header + 12 entries whose shared columns are byte-equal to the DC ingest's, with every `mandate_ref` NULL.
- `EndoDcIdentityTest` (4): both books read through the same resolver. Record 0 of each is 169 bytes and still resolves to `Layouts.HEADER`, which only its INDEX can decide; the two headers are byte-identical except `tx_count`; detail 1 slices identically across both. The fields the fixtures deliberately differ on are asserted DIFFERENT, so none of the equalities can pass by both paths reading one file.
- `PrrPartitionDeterminismTest`: the same fixture under `dcre.prr.max-partitions` 1 vs 5 in two `@Nested` contexts yields identical `(sequence, e2e, content_hash)` rows (R-41 determinism).
- `PrrJobConfigRetryTest`: headerStep re-runs the tasklet on commit-time CRDB 40001 serialization aborts.
- `LineRangePartitionerTest` / `FixedRecordRangeReaderTest`: byte-offset ranges, missing-final-newline, ragged-length and wrong-separator fail-closed, mid-range restart.
- `SpineWriterTest`: content hash covers the essential business fields only; V3 `mandate_ref` mapping.
- Cucumber BDD suite (`CucumberSuiteTest`, `features/prr_boundary_reader.feature`, tag `@prr`): business-language scenarios over the real job + CockroachDB, including the no-flow-column schema invariant.

Fixtures: `src/test/resources/dcre_copybook_v{1,2,3}_*.txt`, byte-identical copies of the toolkit samples, including `dcre_copybook_v2_endo_sample.txt` and its manifest. The DC files keep their `_dc_` names because they ARE the byte-identical OnHost copybook both families use; renaming them would imply a payments-specific layout that does not exist. Carrying only the DC ones would have been worse: a fixture set with one value in the field that carries the distinction cannot exercise what that field drives, which is why the ENDO book is committed rather than merely referenced.

## Local cluster deployment

Both commands below have been run against `dcre-dev` in this exact form, from a state where the
image was absent from the node, and the image is on the node afterwards:

```bash
./gradlew bootBuildImage                            # -> dcre-prr:2.0, via Paketo buildpacks
kind load docker-image --name dcre-dev dcre-prr:2.0
```

**PRR builds its image with Paketo buildpacks and ships no Dockerfile.** The estate mandate is that all backend images go through Paketo, never a hand-rolled prod JVM Dockerfile, and a brand-new repo with zero tags and no published image is the cheapest adoption point in the fleet: nothing has to stay byte-compatible and there is no rollback to preserve. The rest of the DCRE fleet still ships CRR's hand-rolled `eclipse-temurin:25-jre-alpine` Dockerfile and migrates separately; PRR is the exemplar that migration follows.

Both `builder` and `BP_JVM_VERSION` are pinned in `build.gradle`. The builder pin matters even though
it resolves identically today: unset, it comes from the Boot plugin's FLOATING default, so a plugin
bump would silently change the base image of the repo whose job is to be the exemplar. Verified in
the built image: `Starting PrrApplication v2.0 using Java 25.0.4`, and pinning the builder produced a
byte-identical image ID, so the pin is a no-op now and a guard later.

The image is `linux/arm64`, matching the `dcre-dev` kind nodes. Do NOT force `--platform linux/amd64`
for this cluster: its nodes are arm64 and an amd64 image fails at pull time with
`no match for platform in manifest`, minutes after a release looks complete.

### Why `kind load docker-image` works here when the estate says it fails

`dcre-infra/scripts/kind-up.sh` records that `kind load docker-image` fails against Docker's
containerd image store, because a **multi-arch index** references per-platform blobs that
`docker save` omits (`ctr: content digest ... not found`, kind#3510). That note is correct and it
does not apply to this image. This daemon IS on the containerd store
(`driver-type io.containerd.snapshotter.v1`), and the plain form still succeeds, because a
`bootBuildImage` image is built for one platform only: its archive carries a single manifest, so
every blob the index references is present.

The archive form the sibling script uses also works and is the fallback if you ever publish a
multi-arch PRR image:

```bash
docker save --platform "linux/$(docker version --format '{{.Server.Arch}}')" dcre-prr:2.0 \
  | kind load image-archive /dev/stdin --name dcre-dev
```

Both forms were executed, each from a node state where the image had been removed first, so neither
result is a no-op. The on-node image ID differs from the local one (`857103cdfa03` vs `b8dc46a17d2c`)
because containerd recomputes the config digest on import; the labels, architecture and entrypoint
match the local image exactly.

Size, on the node, is the number that matters for load and pull time: `dcre-prr:2.0` is **326MB**
against the hand-rolled fleet images' **95.3MB**. That is the real cost of the reproducible build
and the SBOM, and it is worth knowing before the fleet migration rather than after it.

The image is tagged from the Gradle project version, which stays `2.0` to match uniform fleet versioning. Release image tags are cut by the fleet release process; this repo has no tags yet, so there is no `2.x.y` image tag to build here until it does.

**`dcre-infra/README.md` publishes a fleet build recipe (`./gradlew bootJar && docker build ...`)
that does not work for PRR**, since there is no Dockerfile here. That is a documentation
inconsistency, not a broken pipeline: no script, yml or Makefile in the DCRE tree or in dcre-infra
references a per-repo Dockerfile, so nothing silently skips PRR. The correction to that file belongs
to its owner and is routed separately.

AGT launches PRR as an ephemeral K8s Job per registered ENDO arrival: the JobParameters arrive as program args and `JOB_NAME` is set in the Job env, and the Paketo launcher passes them through unchanged (verified by running the image with all three). **The AGT wiring and the emission-gate flip are NOT part of this phase**; PRR is buildable and verifiable standalone, and AGT still routes ENDO arrivals to CRR until that work lands.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
