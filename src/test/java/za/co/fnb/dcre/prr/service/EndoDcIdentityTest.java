package za.co.fnb.dcre.prr.service;

import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.platform.copybook.CopybookReader;
import za.co.fnb.dcre.platform.copybook.FixedWidthRecord;
import za.co.fnb.dcre.platform.copybook.Layouts;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The claim {@link PaymentRecords} makes in prose, made falsifiable.
 *
 * <p>PRR keeps the SHARED {@code Layouts} tables rather than forking a
 * payments-only copybook, and the justification is that an ENDO payment book and
 * a DC collection book are the same physical OnHost record. The toolkit is the
 * authority: {@code generate_dcre_copybook.py} takes {@code --flow dc|endo} and
 * documents that "Physical DC/ENDO distinction is provenance-level (MFT route),
 * so it lives in the manifest only."
 *
 * <p>Until this class existed that claim was asserted in three javadocs, a README
 * and a commit message, and tested NOWHERE, because every PRR fixture was the
 * collections file (review finding I3: fixture monoculture). A fixture whose every
 * row shares one value cannot exercise what that value drives. Both books are now
 * committed and read through the same resolver here, so the day OnHost genuinely
 * diverges this goes red instead of the prose quietly becoming false.
 *
 * <p>Fixtures are byte-identical copies of the toolkit samples
 * {@code dcre_copybook_v2_{dc,endo}_sample.txt}; the ENDO manifest is committed
 * beside them as the provenance record that the distinction lives in.
 */
class EndoDcIdentityTest {

    private static final Path DC = Path.of("src/test/resources/dcre_copybook_v2_dc_sample.txt");
    private static final Path ENDO = Path.of("src/test/resources/dcre_copybook_v2_endo_sample.txt");

    /** Header fields that must be byte-identical across the two books. */
    private static final List<String> SHARED_HEADER_FIELDS = List.of(
            "record_type", "sender_id", "file_type", "created_ts", "layout_version",
            "filler_1", "created_short", "destination_id", "filler_2", "business_date");

    /** Detail-1 fields the two fixtures generate identically. */
    private static final List<String> SHARED_DETAIL_FIELDS = List.of(
            "record_type", "end_to_end", "creditor_account", "currency",
            "branch_code", "debtor_name", "debtor_account", "acc_type_seq");

    private static List<FixedWidthRecord> read(final Path file) throws IOException {
        return CopybookReader.read(file, PaymentRecords.INSTANCE);
    }

    /**
     * The load-bearing index, on BOTH books. Every record in both files is 169
     * bytes, because the generator pads the header out to the detail LRECL, so
     * length alone cannot tell record 0 from a V2 detail. Only its position can.
     */
    @Test
    void bothBooksResolveRecordZeroAsHeaderDespiteBeingDetailLength() throws Exception {
        for (Path book : List.of(DC, ENDO)) {
            List<FixedWidthRecord> records = read(book);
            assertEquals(Layouts.DETAIL_V2.length(), records.get(0).line().length(),
                    book + ": record 0 must be padded to the detail LRECL for this test to mean anything");
            assertSame(Layouts.HEADER, records.get(0).layout(),
                    book + ": record 0 is the header, and only its INDEX says so");
            for (FixedWidthRecord detail : records.subList(1, records.size())) {
                assertSame(Layouts.DETAIL_V2, detail.layout(),
                        book + ": record " + detail.index() + " must cut as a V2 detail");
            }
        }
    }

    /**
     * The identity claim on the header: one physical shape, differing only in how
     * many records follow. tx_count is asserted DIFFERENT so the test cannot pass
     * by both paths accidentally reading the same file.
     */
    @Test
    void headersAreIdenticalExceptForTheRecordCount() throws Exception {
        FixedWidthRecord dc = read(DC).get(0);
        FixedWidthRecord endo = read(ENDO).get(0);
        for (String field : SHARED_HEADER_FIELDS) {
            assertEquals(dc.field(field), endo.field(field),
                    "header field " + field + " must be byte-identical across DC and ENDO");
        }
        assertEquals("00000000000030", dc.field("tx_count"));
        assertEquals("00000000000012", endo.field("tx_count"));
        assertNotEquals(dc.field("tx_count"), endo.field("tx_count"),
                "the two books must be genuinely different files, or every assertion above is vacuous");
    }

    /**
     * The identity claim on a detail record: the same columns carry the same
     * roles, sliced by the same table. The two fields the fixtures deliberately
     * generate differently are asserted different for the same anti-vacuity
     * reason as above.
     */
    @Test
    void firstDetailRecordSlicesIdenticallyAcrossBothBooks() throws Exception {
        FixedWidthRecord dc = read(DC).get(1);
        FixedWidthRecord endo = read(ENDO).get(1);
        for (String field : SHARED_DETAIL_FIELDS) {
            assertEquals(dc.field(field), endo.field(field),
                    "detail field " + field + " must slice identically across DC and ENDO");
        }
        assertNotEquals(dc.field("amount"), endo.field("amount"),
                "the fixtures carry different money, so a passing comparison above is a real one");
        assertNotEquals(dc.field("contract_ref"), endo.field("contract_ref"),
                "the fixtures carry different contract refs");
    }

    /** Record counts, so a truncated fixture cannot silently shrink the sweep. */
    @Test
    void bothBooksCarryTheirDeclaredRecordCounts() throws Exception {
        assertEquals(31, read(DC).size(), "DC book: 1 header + 30 details");
        assertEquals(13, read(ENDO).size(), "ENDO book: 1 header + 12 details");
    }
}
