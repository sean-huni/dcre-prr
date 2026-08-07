package za.co.fnb.dcre.prr.bdd;

import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Same Spring Boot test setup as PrrJobTest: one static CockroachDB container
 * per context, batch metadata database pre-created before the context wires.
 */
@CucumberContextConfiguration
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
public class CucumberSpringConfig {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
        // create the metadata database before the context wires the batch DS
        try (var conn = java.sql.DriverManager.getConnection(CRDB.getJdbcUrl(), CRDB.getUsername(), CRDB.getPassword())) {
            conn.createStatement().execute("CREATE DATABASE IF NOT EXISTS prr_meta");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
        // second database for batch metadata on the same container
        registry.add("prr.batch.datasource.url",
                () -> CRDB.getJdbcUrl().replace("/" + CRDB.getDatabaseName(), "/prr_meta"));
    }
}
