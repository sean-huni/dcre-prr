package za.co.fnb.dcre.prr.service;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

/** Thin entry adapter (3-tier, configuration.md point 21). */
@Component
public class HeaderTasklet implements Tasklet {

    public static final String EXIT_FILE_FATAL = "FILE_FATAL";

    /** The launch parameter naming the file to read; AGT supplies it on every launch. */
    static final String INPUT_FILE = "input.file";

    private final HeaderService service;

    public HeaderTasklet(HeaderService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        Optional<String> fatal = service.ingestHeader(
                UUID.fromString((String) params.get("arrival.id")),
                Path.of(requiredInputFile((String) params.get(INPUT_FILE))),
                (String) params.get("original.name"));
        if (fatal.isPresent()) {
            chunkContext.getStepContext().getStepExecution().getJobExecution()
                    .getExecutionContext().putString("fileFatalReason", fatal.get());
            contribution.setExitStatus(new ExitStatus(EXIT_FILE_FATAL, fatal.get()));
        }
        return RepeatStatus.FINISHED;
    }

    /**
     * Names the missing parameter, and this stage, before the value can reach {@link Path#of}.
     * Unguarded, a launch without {@code input.file} dies inside the JDK's filesystem code with a
     * NullPointerException that names neither the parameter, nor this stage, nor the job, so the
     * operator reading that log learns nothing about what to supply. Blank counts as missing: a
     * blank string builds an empty path with no NullPointerException at all and then fails
     * somewhere else entirely, which is the quieter half of the same defect.
     *
     * <p>Deliberately NOT a {@code FileFatalException}: that type is a BUSINESS verdict about the
     * file's contents and is routed to a NACK. A parameter the launch never supplied is an
     * operator error about the launch, and there is no file to pass judgement on.
     */
    private static String requiredInputFile(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("PRR requires the job parameter '" + INPUT_FILE
                    + "': it was " + (value == null ? "not supplied" : "blank ('" + value + "')")
                    + ". Launch the job with " + INPUT_FILE + "=<path to the file to ingest>.");
        }
        return value;
    }
}
