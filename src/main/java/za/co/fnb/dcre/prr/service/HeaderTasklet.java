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

    private final HeaderService service;

    public HeaderTasklet(HeaderService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        var params = chunkContext.getStepContext().getJobParameters();
        Optional<String> fatal = service.ingestHeader(
                UUID.fromString((String) params.get("arrival.id")),
                Path.of((String) params.get("input.file")),
                (String) params.get("original.name"));
        if (fatal.isPresent()) {
            chunkContext.getStepContext().getStepExecution().getJobExecution()
                    .getExecutionContext().putString("fileFatalReason", fatal.get());
            contribution.setExitStatus(new ExitStatus(EXIT_FILE_FATAL, fatal.get()));
        }
        return RepeatStatus.FINISHED;
    }
}
