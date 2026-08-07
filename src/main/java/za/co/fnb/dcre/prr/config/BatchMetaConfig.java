package za.co.fnb.dcre.prr.config;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import za.co.fnb.dcre.platform.batch.StaleExecutionSweeper;

import javax.sql.DataSource;

/**
 * A-39a: abandon stale STARTED executions in THIS service's prefixed Batch
 * metadata before the job runner fires, so a relaunch after a pod kill never
 * throws JobExecutionAlreadyRunning. AGT never touches service metadata.
 */
@Configuration
public class BatchMetaConfig {

    @Bean
    @Order(-10) // before JobLauncherApplicationRunner
    public ApplicationRunner staleExecutionSweep(DataSource dataSource) {
        return args -> StaleExecutionSweeper.abandonStale(dataSource, "PRR_BATCH_", 60);
    }
}
