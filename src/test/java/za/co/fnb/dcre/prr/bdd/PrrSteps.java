package za.co.fnb.dcre.prr.bdd;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step definitions for the PRR boundary reader. Input files are derived from
 * the committed V2/V1 samples; header mutations reuse the Layouts offsets
 * (tx_count occupies columns 52-66, header content length 109).
 */
public class PrrSteps {

    static final String V2_SAMPLE = "dcre_copybook_v2_dc_sample.txt";
    static final String V1_SAMPLE = "dcre_copybook_v1_legacy_sample.txt";
    static final String V2_NAME = "FNBRF01_DCRERF2026071112000002.txt";
    static final String V1_NAME = "FNBRF01_DCRERF2026071112000001.txt";
    static final int TX_COUNT_START = 52;
    static final int TX_COUNT_END = 66;

    @Autowired
    Job prrJob;

    @Autowired
    JobOperator jobOperator;

    @Autowired
    JdbcTemplate jdbc;

    UUID arrival;
    Path inputFile;
    String originalName;
    JobExecution execution;

    @Given("the boundary reader receives the standard V2 payment file")
    public void standardV2File() throws Exception {
        arrival = UUID.randomUUID();
        inputFile = sample(V2_SAMPLE);
        originalName = V2_NAME;
    }

    @Given("the boundary reader receives the legacy V1 payment file")
    public void legacyV1File() throws Exception {
        arrival = UUID.randomUUID();
        inputFile = sample(V1_SAMPLE);
        originalName = V1_NAME;
    }

    @Given("the boundary reader receives the V2 file with a header declaring {int} transactions")
    public void v2FileWithWrongCount(int declared) throws Exception {
        arrival = UUID.randomUUID();
        List<String> lines = Files.readAllLines(sample(V2_SAMPLE));
        String header = lines.get(0);
        String mutated = header.substring(0, TX_COUNT_START)
                + String.format("%014d", declared)
                + header.substring(TX_COUNT_END);
        inputFile = writeInput("wrong-count", mutated, lines.subList(1, lines.size()));
        originalName = V2_NAME;
    }

    @Given("the boundary reader receives the V2 file with a truncated header")
    public void v2FileWithTruncatedHeader() throws Exception {
        arrival = UUID.randomUUID();
        List<String> lines = Files.readAllLines(sample(V2_SAMPLE));
        inputFile = writeInput("short-header", lines.get(0).substring(0, 80), lines.subList(1, lines.size()));
        originalName = V2_NAME;
    }

    @Given("the boundary reader receives the V2 file with a truncated final detail")
    public void v2FileWithRaggedFinalDetail() throws Exception {
        // The header and every earlier record are intact; only the LAST detail is
        // cut short, to 100 bytes, which matches no layout. Before SCRUM-107 this
        // reached LineRangePartitioner; it is now caught at the header stage, and
        // the reason string a client receives is what this scenario pins.
        arrival = UUID.randomUUID();
        List<String> lines = Files.readAllLines(sample(V2_SAMPLE));
        List<String> details = new java.util.ArrayList<>(lines.subList(1, lines.size()));
        int last = details.size() - 1;
        details.set(last, details.get(last).substring(0, 100));
        inputFile = writeInput("ragged-detail", lines.get(0), details);
        originalName = V2_NAME;
    }

    @Given("the file arrived under the name {string}")
    public void arrivedUnderName(String name) {
        originalName = name;
    }

    @When("the PRR job runs")
    public void prrJobRuns() throws Exception {
        execution = jobOperator.start(prrJob, params(null));
    }

    @When("the PRR job runs again for the same arrival")
    public void prrJobRunsAgain() throws Exception {
        // new attempt parameter: a fresh JobInstance re-processing the same
        // arrival exercises the ON CONFLICT upsert path, not instance refusal
        execution = jobOperator.start(prrJob, params("2"));
    }

    @Then("the job completes with a clean business verdict")
    public void jobCompletesClean() {
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        assertFalse(execution.getExecutionContext().containsKey("fileFatalReason"),
                "no file-fatal reason expected");
    }

    @Then("the file is rejected file-fatally with a reason containing {string}")
    public void rejectedFileFatally(String reasonPart) {
        // FILE_FATAL is a business verdict, not a crash: the job COMPLETES
        assertEquals(BatchStatus.COMPLETED, execution.getStatus());
        String reason = execution.getExecutionContext().getString("fileFatalReason");
        assertTrue(reason.contains(reasonPart),
                "expected file-fatal reason containing '" + reasonPart + "' but was: " + reason);
    }

    @Then("the spine holds one header row and {int} entry rows for the arrival")
    public void spineHoldsRows(int entries) {
        assertEquals(1, jdbc.queryForObject(
                "SELECT count(*) FROM tx_header WHERE arrival_id=?", Integer.class, arrival));
        assertEquals(entries, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, arrival));
    }

    /**
     * The split asserted against the SCHEMA, not against a value. CRR's spine
     * answers this query with one row; the payments spine must answer with
     * none, because the database it lives in IS the discriminator.
     */
    @Then("the tx_header table has no flow column")
    public void txHeaderHasNoFlowColumn() {
        assertEquals(0, jdbc.queryForObject("""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'tx_header' AND column_name = 'flow'""", Integer.class));
        // Control: the query CAN see this table's columns, so the zero above is
        // an absent column and not a mistyped table or an empty catalogue read.
        assertEquals(1, jdbc.queryForObject("""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = 'tx_header' AND column_name = 'arrival_id'""", Integer.class));
    }

    @Then("no spine entries are persisted for the arrival")
    public void noSpineEntries() {
        assertEquals(0, jdbc.queryForObject(
                "SELECT count(*) FROM tx_entry WHERE arrival_id=?", Integer.class, arrival));
    }

    @Then("every persisted amount equals its raw digits scaled to two decimals")
    public void amountsMatchRawDigits() {
        List<java.util.Map<String, Object>> rows = jdbc.queryForList(
                "SELECT amount, amount_raw FROM tx_entry WHERE arrival_id=?", arrival);
        assertFalse(rows.isEmpty(), "expected persisted entries");
        for (var row : rows) {
            BigDecimal amount = (BigDecimal) row.get("amount");
            String raw = (String) row.get("amount_raw");
            assertEquals(new BigDecimal(raw).movePointLeft(2).stripTrailingZeros(),
                    amount.stripTrailingZeros(),
                    "amount parsed via the single MoneyText converter at scale 2");
        }
    }

    JobParameters params(String attempt) {
        JobParametersBuilder builder = new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("input.file", inputFile.toAbsolutePath().toString(), false)
                .addString("original.name", originalName, false);
        if (attempt != null) {
            builder.addString("attempt", attempt, true);
        }
        return builder.toJobParameters();
    }

    Path sample(String name) {
        return Path.of("src/test/resources", name).toAbsolutePath();
    }

    Path writeInput(String label, String header, List<String> details) throws Exception {
        Path dir = Path.of("build/bdd-input");
        Files.createDirectories(dir);
        Path file = dir.resolve(label + "-" + arrival + ".txt");
        Files.write(file, concat(header, details));
        return file.toAbsolutePath();
    }

    static List<String> concat(String header, List<String> details) {
        java.util.ArrayList<String> all = new java.util.ArrayList<>();
        all.add(header);
        all.addAll(details);
        return all;
    }
}
