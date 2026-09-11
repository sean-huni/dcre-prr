package za.co.fnb.dcre.prr.batch;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.prr.service.HeaderTasklet;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Red-proofs the {@code input.file} guard on BOTH of this service's sites, with no container and
 * no database. PRR builds a path from that parameter twice, in two layers, and a guard on one
 * of them is a guard on neither: the partitioner is constructed by step scope and the header
 * tasklet by the step itself, so a launch missing the parameter reaches whichever runs first.
 *
 * <p>Measured on a live cluster on 2026-09-11 against the sibling reader stages: launched without
 * the parameter, the stage ran its step and died with a NullPointerException raised inside
 * {@code sun.nio.fs.UnixFileSystem.getPath}, naming neither the parameter, nor the stage, nor the
 * job. The cases below assert the MESSAGE, not merely that something was thrown: a guard asserted
 * as "throws" passes for every reason a method can fail, the NullPointerException included.
 *
 * <p>Each pair carries a control asserting that a PRESENT value still reaches the filesystem
 * exactly as before. Without it, a guard that rejected every value, valid ones included, would
 * pass this class.
 */
class InputFileParameterGuardTest {

    private static final String ABSENT_FILE = "/dcre/no/such/file/prr-guard-control.txt";

    private static void executeHeader(final JobParameters parameters) throws Exception {
        var repository = new ResourcelessJobRepository();
        JobInstance instance = repository.createJobInstance("prrJob", parameters);
        JobExecution jobExecution =
                repository.createJobExecution(instance, parameters, new ExecutionContext());
        StepExecution stepExecution = repository.createStepExecution("headerStep", jobExecution);
        new HeaderTasklet(null).execute(new StepContribution(stepExecution),
                new ChunkContext(new StepContext(stepExecution)));
    }

    @Test
    void theHeaderTaskletNamesAnAbsentParameterAndTheStage() {
        JobParameters parameters = new JobParametersBuilder()
                .addString("arrival.id", "11111111-2222-3333-4444-555555555555")
                .addString("original.name", "whatever.txt").toJobParameters();

        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> executeHeader(parameters));

        assertEquals("PRR requires the job parameter 'input.file': it was not supplied."
                        + " Launch the job with input.file=<path to the file to ingest>.",
                thrown.getMessage(),
                "the message must name the parameter and the stage; the NullPointerException it"
                        + " replaces named neither");
    }

    @Test
    void theHeaderTaskletTreatsBlankAsMissing() {
        JobParameters parameters = new JobParametersBuilder()
                .addString("arrival.id", "11111111-2222-3333-4444-555555555555")
                .addString("input.file", " ")
                .addString("original.name", "whatever.txt").toJobParameters();

        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> executeHeader(parameters));

        assertEquals("PRR requires the job parameter 'input.file': it was blank (' ')."
                        + " Launch the job with input.file=<path to the file to ingest>.",
                thrown.getMessage(),
                "blank must be rejected by the guard: it reaches Path.of without a"
                        + " NullPointerException and then fails somewhere else entirely");
    }

    @Test
    void theHeaderTaskletStillPassesASuppliedValueStraightThrough() {
        JobParameters parameters = new JobParametersBuilder()
                .addString("arrival.id", "11111111-2222-3333-4444-555555555555")
                .addString("input.file", ABSENT_FILE)
                .addString("original.name", "whatever.txt").toJobParameters();

        // A bare "throws NullPointerException" would be ambiguous here, because the defect this
        // guard removes is ALSO a NullPointerException. The helpful message is what separates
        // them: Path.of's comes from Objects.requireNonNull and carries none, while this one
        // names the call that was reached only because the guard passed the value through.
        NullPointerException thrown =
                assertThrows(NullPointerException.class, () -> executeHeader(parameters));

        assertTrue(String.valueOf(thrown.getMessage()).contains("ingestHeader"),
                "a present value must pass the guard and reach the (deliberately null) service."
                        + " Expected a NullPointerException naming ingestHeader, got: "
                        + thrown.getMessage());
    }

    @Test
    void thePartitionerNamesAnAbsentParameterAndTheStage() {
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> new LineRangePartitioner(null));

        assertEquals("PRR requires the job parameter 'input.file': it was not supplied."
                        + " Launch the job with input.file=<path to the file to partition>.",
                thrown.getMessage(),
                "the partitioner is a second site of the same defect and must name the same"
                        + " parameter");
    }

    @Test
    void thePartitionerTreatsBlankAsMissing() {
        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, () -> new LineRangePartitioner("\t"));

        assertEquals("PRR requires the job parameter 'input.file': it was blank ('\t')."
                        + " Launch the job with input.file=<path to the file to partition>.",
                thrown.getMessage(),
                "whitespace-only is missing too; Path.of accepts it and the failure then surfaces"
                        + " somewhere else entirely");
    }

    @Test
    void thePartitionerStillConstructsFromASuppliedValue() {
        assertNotNull(new LineRangePartitioner(ABSENT_FILE),
                "construction must not validate the file's EXISTENCE: partition() is what reads it,"
                        + " and moving that check into the constructor would be a behaviour change");
    }
}
