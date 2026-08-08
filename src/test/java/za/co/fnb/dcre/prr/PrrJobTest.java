package za.co.fnb.dcre.prr;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {"spring.batch.job.enabled=false"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PrrJobTest {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    // Own exchange root per suite run: the seam name falls back to
    // local-prr-<executionId>, and StagedWrite (R-24) is exists->skip, so a
    // prior run's file left under a shared build dir would mask this run's
    // verdict. A fresh temp dir isolates each run; clearOutcomes() below then
    // isolates the individual seam assertions within the run.
    static final Path EXCHANGE_ROOT;

    static {
        CRDB.start();
        try {
            EXCHANGE_ROOT = Files.createTempDirectory("prr-seam-exchange-");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
        registry.add("dcre.exchange-root", EXCHANGE_ROOT::toString);
        // second database for batch metadata on the same container
        registry.add("prr.batch.datasource.url",
                () -> CRDB.getJdbcUrl().replace("/" + CRDB.getDatabaseName(), "/prr_meta"));
    }

    /**
     * Isolate a seam assertion from any earlier job's outcome file: the fallback
     * seam name local-prr-&lt;executionId&gt; can repeat across the suite's job
     * runs, and StagedWrite never overwrites an existing target, so a stale file
     * would silently keep a prior verdict. Clearing the outcomes dir before the
     * run under test guarantees this run's verdict is the one written and read.
     */
    void clearOutcomes() throws IOException {
        Path outcomes = EXCHANGE_ROOT.resolve("outcomes");
        if (Files.isDirectory(outcomes)) {
            try (var entries = Files.newDirectoryStream(outcomes)) {
                for (Path entry : entries) {
                    Files.deleteIfExists(entry);
                }
            }
        }
    }

    @Autowired
    Job prrJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    JdbcTemplate jdbc;

    static final UUID ARRIVAL = UUID.randomUUID();
    static final UUID V3_ARRIVAL = UUID.randomUUID();
    static final UUID ENDO_ARRIVAL = UUID.randomUUID();

    static {
        // create the metadata database before the context wires the batch DS
        try (var conn = java.sql.DriverManager.getConnection(CRDB.getJdbcUrl(), CRDB.getUsername(), CRDB.getPassword())) {
            conn.createStatement().execute("CREATE DATABASE IF NOT EXISTS prr_meta");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    JobParameters params(UUID arrival, String file, String name) {
        return new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("input.file", Path.of("src/test/resources", file).toAbsolutePath().toString(), false)
                .addString("original.name", name, false)
                .toJobParameters();
    }

    @Test
    @Order(1)
    void parsesDcSampleIntoSpine() throws Exception {
        JobExecution run = jobOperator.start(prrJob,
                params(ARRIVAL, "dcre_copybook_v2_dc_sample.txt", "FNBRF01_DCRERF2026071112000002.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tx_header WHERE arrival_id=?", Integer.class, ARRIVAL));
        assertEquals(30, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, ARRIVAL));
        BigDecimal amount = jdbc.queryForObject(
                "SELECT amount FROM tx_entry WHERE arrival_id=? AND sequence=1", BigDecimal.class, ARRIVAL);
        String raw = jdbc.queryForObject(
                "SELECT amount_raw FROM tx_entry WHERE arrival_id=? AND sequence=1", String.class, ARRIVAL);
        assertEquals(new BigDecimal(raw).movePointLeft(2).stripTrailingZeros(), amount.stripTrailingZeros(),
                "amount parsed via the single MoneyText converter at scale 2");
    }

    @Test
    @Order(2)
    void rerunSameIdentityDoesNotDuplicate() {
        // R-05/R-16: same identifying parameters = same JobInstance; a completed
        // instance refuses a second run and the spine stays exactly 30 rows.
        var thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () ->
                jobOperator.start(prrJob,
                        params(ARRIVAL, "dcre_copybook_v2_dc_sample.txt", "FNBRF01_DCRERF2026071112000002.txt")));
        assertTrue(thrown.getClass().getSimpleName().contains("JobInstanceAlreadyComplete")
                        || String.valueOf(thrown.getMessage()).contains("already"),
                "unexpected: " + thrown);
        assertEquals(30, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, ARRIVAL));
    }

    @Test
    @Order(3)
    void v1FailsClosedAsFileFatal() throws Exception {
        UUID v1Arrival = UUID.randomUUID();
        JobExecution run = jobOperator.start(prrJob,
                params(v1Arrival, "dcre_copybook_v1_legacy_sample.txt", "FNBRF01_DCRERF2026071112000001.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus(), "FILE_FATAL is a business verdict, not a crash");
        assertTrue(run.getExecutionContext().containsKey("fileFatalReason"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, v1Arrival),
                "no details persisted for a file-fatal V1 file");
    }

    JobParameters paramsAbsolute(UUID arrival, java.nio.file.Path file, String name) {
        return new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("input.file", file.toAbsolutePath().toString(), false)
                .addString("original.name", name, false)
                .toJobParameters();
    }

    List<String> projection(UUID arrival) {
        return jdbc.queryForList(
                "SELECT sequence || '|' || e2e || '|' || content_hash FROM tx_entry WHERE arrival_id=? ORDER BY sequence",
                String.class, arrival);
    }

    @Test
    @Order(4)
    void unpaddedHeaderStillIngestsAllRecords() throws Exception {
        // real header content is 109 bytes; only generator files pad it to the
        // detail LRECL. Base offset must derive from line 1, stride from line 2.
        byte[] padded = Files.readAllBytes(Path.of("src/test/resources/dcre_copybook_v2_dc_sample.txt"));
        byte[] unpadded = new byte[109 + 1 + (padded.length - 170)];
        System.arraycopy(padded, 0, unpadded, 0, 109);
        unpadded[109] = '\n';
        System.arraycopy(padded, 170, unpadded, 110, padded.length - 170);
        Path file = Path.of("build/test-exchange/unpadded-header.txt");
        Files.createDirectories(file.getParent());
        Files.write(file, unpadded);

        UUID arrival = UUID.randomUUID();
        JobExecution run = jobOperator.start(prrJob,
                paramsAbsolute(arrival, file, "FNBRF01_DCRERF2026071112000002.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        assertEquals(projection(ARRIVAL), projection(arrival),
                "unpadded-header ingest must match the padded-header ingest row for row");
    }

    @Test
    @Order(6)
    void seamFallbackNameIsSelfDescribing() throws Exception {
        // SCRUM-58: a run without JOB_NAME in the env writes the outcome seam
        // under the self-describing fallback local-prr-<executionId>.
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("JOB_NAME") == null,
                "test requires JOB_NAME absent from the environment");
        clearOutcomes();
        UUID arrival = UUID.randomUUID();
        JobExecution run = jobOperator.start(prrJob,
                params(arrival, "dcre_copybook_v2_dc_sample.txt", "FNBRF01_DCRERF2026071112000002.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        Path seam = EXCHANGE_ROOT.resolve("outcomes").resolve("local-prr-" + run.getId());
        assertTrue(Files.exists(seam), "expected self-describing seam file at " + seam);
        assertEquals(List.of("BUSINESS_ACCEPTED"), Files.readAllLines(seam),
                "clean-run business verdict must stay byte-exact");
    }

    @Test
    @Order(7)
    void seamCarriesFileFatalVerdictByteExact() throws Exception {
        // Pins the business verdict the retired PrrJobListener derived from
        // fileFatalReason, now supplied to the shared OutcomeSeamListener.
        org.junit.jupiter.api.Assumptions.assumeTrue(System.getenv("JOB_NAME") == null,
                "test requires JOB_NAME absent from the environment");
        clearOutcomes();
        UUID arrival = UUID.randomUUID();
        JobExecution run = jobOperator.start(prrJob,
                params(arrival, "dcre_copybook_v1_legacy_sample.txt", "FNBRF01_DCRERF2026071112000001.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        Path seam = EXCHANGE_ROOT.resolve("outcomes").resolve("local-prr-" + run.getId());
        assertTrue(Files.exists(seam), "expected self-describing seam file at " + seam);
        assertEquals(List.of("BUSINESS_FILE_FATAL"), Files.readAllLines(seam),
                "file-fatal business verdict must stay byte-exact");
    }

    @Test
    @Order(8)
    void v3BookCarriesMandateRefWhileV2StaysNull() throws Exception {
        // M10: DETAIL_V3 (204 = V2 body + trailing mandate_ref 35) is selected
        // by LRECL. mandate_ref is the COLLECTION-to-mandate link (mandate gate
        // is DC-only, R-19); it is mapped here only because DETAIL_V3 is one
        // physical layout shared by both families. See EndoDcIdentityTest for
        // the ENDO side, where every mandate_ref is NULL.
        JobExecution run = jobOperator.start(prrJob,
                params(V3_ARRIVAL, "dcre_copybook_v3_dc_sample.txt", "FNBRF01_DCRERF2026071112000003.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        assertEquals(30, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, V3_ARRIVAL));
        assertEquals("MND0000000001", jdbc.queryForObject(
                "SELECT mandate_ref FROM tx_entry WHERE arrival_id=? AND sequence=1", String.class, V3_ARRIVAL));
        // 28 rows carry a mandate link; the 2 blank-field V3 rows persist as NULL
        assertEquals(28, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=? AND mandate_ref IS NOT NULL", Integer.class, V3_ARRIVAL));
        assertEquals(2, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=? AND mandate_ref IS NULL", Integer.class, V3_ARRIVAL));
        // back-compat: the V2 book (Order 1) has no mandate_ref field, every row NULL
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=? AND mandate_ref IS NOT NULL", Integer.class, ARRIVAL));
    }

    @Test
    @Order(9)
    void v3RerunSameIdentityDoesNotDuplicate() {
        // R-05/R-16: mandate_ref rides the same guarded upsert keyed
        // (arrival_id, sequence); a completed instance refuses a re-run and the
        // spine stays exactly 30 rows, one per sequence (zero-dup on resume).
        var thrown = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () ->
                jobOperator.start(prrJob,
                        params(V3_ARRIVAL, "dcre_copybook_v3_dc_sample.txt", "FNBRF01_DCRERF2026071112000003.txt")));
        assertTrue(thrown.getClass().getSimpleName().contains("JobInstanceAlreadyComplete")
                        || String.valueOf(thrown.getMessage()).contains("already"),
                "unexpected: " + thrown);
        assertEquals(30, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, V3_ARRIVAL));
        assertEquals(30, jdbc.queryForObject(
                "SELECT count(DISTINCT sequence) FROM tx_entry WHERE arrival_id=?", Integer.class, V3_ARRIVAL));
    }

    @Test
    @Order(5)
    void nonUtf8ByteIngestsByteTransparently() throws Exception {
        // 0xE9 (ISO_8859_1 e-acute) in debtor_name of record 1: strict-UTF-8
        // decoding anywhere in the pipeline crashes or mangles the byte
        byte[] bytes = Files.readAllBytes(Path.of("src/test/resources/dcre_copybook_v2_dc_sample.txt"));
        bytes[170 + 103] = (byte) 0xE9; // detail record 0, first byte of debtor_name [103,138)
        Path file = Path.of("build/test-exchange/iso-byte.txt");
        Files.createDirectories(file.getParent());
        Files.write(file, bytes);

        UUID arrival = UUID.randomUUID();
        JobExecution run = jobOperator.start(prrJob,
                paramsAbsolute(arrival, file, "FNBRF01_DCRERF2026071112000002.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        assertEquals(30, jdbc.queryForObject("SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, arrival));
        String debtorName = jdbc.queryForObject(
                "SELECT debtor_name FROM tx_entry WHERE arrival_id=? AND sequence=1", String.class, arrival);
        assertTrue(debtorName.contains("\u00E9"), "expected byte-transparent e-acute, got: " + debtorName);
    }

    /**
     * Review finding I3: the ENDO book, ingested by the same job into the same
     * spine. Every other fixture in this repo is the collections file, so before
     * this test the "one physical layout, two families" claim that justifies
     * keeping the shared Layouts tables was prose only.
     *
     * <p>EndoDcIdentityTest proves the two books SLICE identically at the reader.
     * This proves the whole job persists them identically: the columns the two
     * fixtures share arrive byte-equal in the database, having gone through
     * layout resolution, the partitioner, the range reader and the upsert.
     */
    @Test
    @Order(10)
    void endoBookIngestsIdenticallyToTheDcBook() throws Exception {
        JobExecution run = jobOperator.start(prrJob,
                params(ENDO_ARRIVAL, "dcre_copybook_v2_endo_sample.txt",
                        "FNBRF01_DCRERF2026071112000004.txt"));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());

        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM tx_header WHERE arrival_id=?", Integer.class, ENDO_ARRIVAL));
        assertEquals(12, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, ENDO_ARRIVAL));

        // The shared columns of detail 1, compared across the two ingests. The DC
        // row is the one written by parsesDcSampleIntoSpine (@Order(1)).
        String projection = """
                SELECT record_type || '|' || e2e || '|' || creditor_account || '|' || currency
                       || '|' || branch_code || '|' || debtor_name || '|' || debtor_account
                       || '|' || acc_type_seq
                FROM tx_entry WHERE arrival_id=? AND sequence=1""";
        String dcRow = jdbc.queryForObject(projection, String.class, ARRIVAL);
        String endoRow = jdbc.queryForObject(projection, String.class, ENDO_ARRIVAL);
        // Four of the eight concatenated columns are nullable, and `||` with any
        // NULL yields NULL in CockroachDB, so a mapping regression that NULLed one
        // for BOTH books would make the comparison below assertEquals(null, null)
        // and pass. The amount_raw control further down cannot see that mode:
        // amount_raw is NOT NULL. These two assertions are what close it.
        assertNotNull(dcRow, "the DC projection must not collapse to NULL: a single"
                + " NULL column NULLs the whole || concatenation");
        assertNotNull(endoRow, "the ENDO projection must not collapse to NULL");
        assertEquals(dcRow, endoRow,
                "the same physical record must persist to the same column values whichever"
                        + " family's book carried it");

        // Anti-vacuity: the two books are genuinely different files. Without this
        // the comparison above would also pass if both arrivals read one fixture.
        assertNotEquals(
                jdbc.queryForObject("SELECT amount_raw FROM tx_entry WHERE arrival_id=? AND sequence=1",
                        String.class, ARRIVAL),
                jdbc.queryForObject("SELECT amount_raw FROM tx_entry WHERE arrival_id=? AND sequence=1",
                        String.class, ENDO_ARRIVAL),
                "the fixtures carry different money, so the equality above is a real one");

        // ENDO carries no bank-registered mandates: the mandate gate is DC-only
        // (R-19), and the toolkit forbids mandate faults on an ENDO book. The V3
        // mandate_ref column exists because the LAYOUT is shared, not because a
        // payment can carry a mandate, so every ENDO row must leave it NULL.
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=? AND mandate_ref IS NOT NULL",
                Integer.class, ENDO_ARRIVAL));
    }
}
