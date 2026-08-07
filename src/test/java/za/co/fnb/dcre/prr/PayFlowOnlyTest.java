package za.co.fnb.dcre.prr;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.prr.data.model.TxHeaderEntity;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PRR reads the ENDO payment flat file ONLY. The whole point of the split is that
 * payments stops sharing a reader with collections, so a flow discriminator in this
 * service would mean the split had not happened.
 *
 * <p>The source scan alone is NOT sufficient and this class does not rely on it.
 * Review finding I2: a scan for three string literals is satisfied by DELETING
 * three literals, not by removing the concept. In CRR the flow token appears in
 * four main sources and those three literals live in only one of them, so a
 * partial strip that left {@code TxHeaderEntity.flow}, the {@code flow} column in
 * {@code TxHeaderRepo}'s upsert, or {@code params.get("flow")} in
 * {@code HeaderTasklet} would have passed a literal-only guard. Each assertion
 * below closes one of those escapes, and each has been seen red on its own.
 */
class PayFlowOnlyTest {

    /**
     * The literal scan. Cheap, and it catches the constants and the validator by
     * name, but see the class javadoc for what it cannot see on its own.
     */
    @Test
    void prrCarriesNoFlowDiscriminator() throws Exception {
        try (var paths = Files.walk(Path.of("src/main/java"))) {
            var offenders = paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> {
                        try {
                            String s = Files.readString(p);
                            return s.contains("FLOW_PAY") || s.contains("validatedFlow")
                                    || s.contains("\"COL\"");
                        } catch (Exception e) {
                            throw new IllegalStateException(p.toString(), e);
                        }
                    })
                    .toList();
            assertThat(offenders)
                    .as("PRR serves one family. A flow branch here means collections"
                            + " logic was carried across instead of left behind")
                    .isEmpty();
        }
    }

    /**
     * The PERSISTED shape, read off the class itself rather than off its text.
     * A field named flow survives any amount of comment rewording, and it is the
     * one that would put the column back into the upsert.
     */
    @Test
    void spineHeaderDeclaresNoFlowField() {
        List<String> fields = Arrays.stream(TxHeaderEntity.class.getDeclaredFields())
                .map(Field::getName)
                .toList();
        assertThat(fields)
                .as("tx_header in dcre_pay has no flow column, so the entity must not"
                        + " declare one: the database is the discriminator now")
                .doesNotContain("flow");
        // Control: this reflection CAN see the entity's fields, so the absence
        // above is a missing field and not an empty read of the wrong class.
        assertThat(fields)
                .as("control: the reflection reads real fields")
                .contains("arrivalId", "layoutVersion");
    }

    /**
     * The RESOURCES, which the java-only walk cannot reach. A flow column
     * reintroduced by a Liquibase changeset, or a flow key wired through
     * application.yml, is exactly as much of a discriminator as a Java constant.
     */
    @Test
    void noResourceReintroducesAFlowColumnOrKey() throws Exception {
        try (var paths = Files.walk(Path.of("src/main/resources"))) {
            var offenders = paths.filter(Files::isRegularFile)
                    .filter(p -> {
                        try {
                            String s = Files.readString(p);
                            return s.contains("name=\"flow\"") || s.contains("columnName=\"flow\"")
                                    || s.contains("flow:") || s.contains("'flow'");
                        } catch (Exception e) {
                            throw new IllegalStateException(p.toString(), e);
                        }
                    })
                    .toList();
            assertThat(offenders)
                    .as("no changeset may add a flow column and no config may carry a"
                            + " flow key: dcre_pay's tx_header has no such column")
                    .isEmpty();
        }
    }

    /**
     * The LAUNCH surface. CRR's tasklet read a flow job parameter that AGT
     * supplied; PRR must not, or a launcher could still hand it one and be
     * silently ignored, which is the worst of both shapes.
     */
    @Test
    void noSourceReadsAFlowJobParameter() throws Exception {
        try (var paths = Files.walk(Path.of("src/main/java"))) {
            var offenders = paths.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> {
                        try {
                            String s = Files.readString(p);
                            return s.contains("get(\"flow\")")
                                    || s.contains("jobParameters['flow']")
                                    || s.contains("String flow");
                        } catch (Exception e) {
                            throw new IllegalStateException(p.toString(), e);
                        }
                    })
                    .toList();
            assertThat(offenders)
                    .as("PRR takes no flow launch parameter: there is nothing left to"
                            + " discriminate on, so accepting one would be a lie")
                    .isEmpty();
        }
    }
}
