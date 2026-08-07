package za.co.fnb.dcre.prr;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * PRR reads the ENDO payment flat file ONLY. The whole point of the split is that
 * payments stops sharing a reader with collections, so a flow discriminator in this
 * service would mean the split had not happened.
 */
class PayFlowOnlyTest {

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
}
