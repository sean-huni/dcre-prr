package za.co.fnb.dcre.prr.config;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.support.ResourcelessJobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.support.DefaultTransactionStatus;
import za.co.fnb.dcre.prr.service.HeaderTasklet;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves the shared CRDB 40001 retry handler (platform-batch) is registered
 * on the tx_header WRITE step exactly as the production config builds it:
 * the injected failure is thrown from PlatformTransactionManager.doCommit,
 * the boundary where ingest dies under multi-copybook load. The mechanism
 * itself is proven in CTV; this test pins the PRR wiring.
 *
 * <p>The chunk-oriented detailWorkerStep deliberately has NO such test: see
 * the config JavaDoc for why the handler is not registered there.
 */
class PrrJobConfigRetryTest {

    /** Fails the first {@code failures} COMMITS the way JdbcTransactionManager surfaces a CRDB 40001. */
    static final class CommitFailingTxManager extends ResourcelessTransactionManager {

        private final int failures;
        private int commits;

        CommitFailingTxManager(final int failures) {
            this.failures = failures;
        }

        @Override
        protected void doCommit(final DefaultTransactionStatus status) {
            if (++commits <= failures) {
                throw new CannotAcquireLockException(
                        "JDBC commit; ERROR: restart transaction: RETRY_SERIALIZABLE");
            }
            super.doCommit(status);
        }
    }

    static final class CountingHeaderTasklet extends HeaderTasklet {

        final AtomicInteger executions = new AtomicInteger();

        CountingHeaderTasklet() {
            super(null);
        }

        @Override
        public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) {
            executions.incrementAndGet();
            return RepeatStatus.FINISHED;
        }
    }

    @Test
    void headerStepRetriesCommitTimeSerializationAborts() throws Exception {
        var repo = new ResourcelessJobRepository();
        var tasklet = new CountingHeaderTasklet();
        Step step = new PrrJobConfig().headerStep(repo, new CommitFailingTxManager(2), tasklet);

        JobInstance instance = repo.createJobInstance("retryJob", new JobParameters());
        JobExecution jobExecution = repo.createJobExecution(instance, new JobParameters(), new ExecutionContext());
        StepExecution stepExecution = repo.createStepExecution("headerStep", jobExecution);
        step.execute(stepExecution);

        assertEquals(BatchStatus.COMPLETED, stepExecution.getStatus(),
                "two commit-time 40001 aborts must be retried, not fail the step");
        assertEquals(3, tasklet.executions.get(), "tasklet transaction re-runs once per aborted commit");
    }
}
