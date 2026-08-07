package za.co.fnb.dcre.prr;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * R-41 determinism: the partitioned detailStep must persist EXACTLY the row
 * set the sequential run produces. Two @Nested Spring contexts ingest the
 * same fixture (max-partitions 1 vs 5) into their own arrivals, each stashing
 * its ordered (sequence, e2e, content_hash) projection. The outer @AfterAll
 * proves BOTH contexts ran and asserts the projections are identical: with
 * @Nested classes the enclosing lifecycle fires only after both nested
 * containers complete, so there is no vacuous pass.
 */
class PrrPartitionDeterminismTest {

    /** Real fan-out needs >= 2 CPUs; PartitionSizer clamps gridSize to the pod's CPU count. */
    static final boolean MULTI_CPU = Runtime.getRuntime().availableProcessors() >= 2;

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
        // create the metadata database before any context wires the batch DS
        try (var conn = java.sql.DriverManager.getConnection(CRDB.getJdbcUrl(), CRDB.getUsername(), CRDB.getPassword())) {
            conn.createStatement().execute("CREATE DATABASE IF NOT EXISTS prr_meta");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static void registerDb(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
        registry.add("prr.batch.datasource.url",
                () -> CRDB.getJdbcUrl().replace("/" + CRDB.getDatabaseName(), "/prr_meta"));
    }

    /** projection keyed by max-partitions; both nested contexts contribute. */
    static final Map<Integer, List<String>> PROJECTIONS = new ConcurrentHashMap<>();

    @AfterAll
    static void bothContextsRanAndAgreed() {
        assertEquals(2, PROJECTIONS.size(),
                "both nested contexts must run: the cross-comparison is only meaningful with 2 projections");
        assertEquals(30, PROJECTIONS.get(1).size());
        assertEquals(PROJECTIONS.get(1), PROJECTIONS.get(5),
                "partitioned ingest deviates from sequential ingest");
    }

    static JobParameters params(UUID arrival) {
        return new JobParametersBuilder()
                .addString("arrival.id", arrival.toString(), true)
                .addString("input.file", Path.of("src/test/resources",
                        "dcre_copybook_v2_dc_sample.txt").toAbsolutePath().toString(), false)
                .addString("original.name", "FNBRF01_DCRERF2026071112000002.txt", false)
                .toJobParameters();
    }

    static void ingest(int maxPartitions, Job job, JobOperator operator, JdbcTemplate jdbc)
            throws Exception {
        UUID arrival = UUID.randomUUID();
        JobExecution run = operator.start(job, params(arrival));
        assertEquals(BatchStatus.COMPLETED, run.getStatus());
        long workers = run.getStepExecutions().stream()
                .filter(s -> s.getStepName().startsWith("detailWorkerStep:partition"))
                .count();
        if (maxPartitions == 1) {
            assertEquals(1, workers, "max-partitions=1 must collapse to one worker");
        } else if (MULTI_CPU) {
            assertTrue(workers >= 2, "expected a real fan-out, got " + workers + " worker(s)");
        }
        List<String> rows = jdbc.queryForList("""
                SELECT sequence || '|' || e2e || '|' || content_hash
                FROM tx_entry WHERE arrival_id = ? ORDER BY sequence""", String.class, arrival);
        assertEquals(30, rows.size());
        PROJECTIONS.put(maxPartitions, rows);
    }

    @Nested
    @SpringBootTest(properties = {"spring.batch.job.enabled=false",
            "dcre.exchange-root=build/test-exchange", "dcre.prr.max-partitions=1"})
    class SequentialIngest {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) {
            registerDb(registry);
        }

        @Autowired Job prrJob;
        @Autowired JobOperator jobOperator;
        @Autowired JdbcTemplate jdbc;

        @Test
        void ingestsFixture() throws Exception {
            ingest(1, prrJob, jobOperator, jdbc);
        }
    }

    @Nested
    @SpringBootTest(properties = {"spring.batch.job.enabled=false",
            "dcre.exchange-root=build/test-exchange", "dcre.prr.max-partitions=5"})
    class PartitionedIngest {

        @DynamicPropertySource
        static void props(DynamicPropertyRegistry registry) {
            registerDb(registry);
        }

        @Autowired Job prrJob;
        @Autowired JobOperator jobOperator;
        @Autowired JdbcTemplate jdbc;

        @Test
        void ingestsFixture() throws Exception {
            ingest(5, prrJob, jobOperator, jdbc);
        }
    }
}
