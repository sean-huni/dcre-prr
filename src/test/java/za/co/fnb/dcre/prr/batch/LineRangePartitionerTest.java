package za.co.fnb.dcre.prr.batch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import za.co.fnb.dcre.prr.service.FileFatalException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** R-41: detail records split into contiguous [fromRecord, toRecord) ranges. */
class LineRangePartitionerTest {

    private static long from(ExecutionContext c) { return c.getLong("fromRecord"); }
    private static long to(ExecutionContext c) { return c.getLong("toRecord"); }

    private static final String HEADER_PADDED = "H".repeat(169);
    private static final String HEADER_UNPADDED = "H".repeat(109);
    private static final String DETAIL = "D".repeat(169);

    private static Path file(Path dir, String content) throws IOException {
        Path f = dir.resolve("fixture.txt");
        Files.writeString(f, content, StandardCharsets.ISO_8859_1);
        return f;
    }

    @Test
    void missingTrailingNewlineKeepsLastRecord(@TempDir Path dir) throws IOException {
        // generator files end with a newline, but a byte-exact split must not
        // floor-drop the final record when one is missing
        Path f = file(dir, HEADER_PADDED + "\n" + DETAIL + "\n" + DETAIL + "\n" + DETAIL);
        Map<String, ExecutionContext> parts = new LineRangePartitioner(f.toString()).partition(1);
        assertEquals(1, parts.size());
        assertEquals(0, from(parts.get("partition0")));
        assertEquals(3, to(parts.get("partition0")));
    }

    @Test
    void unpaddedHeaderDerivesStrideFromDetailLine(@TempDir Path dir) throws IOException {
        // header 109 bytes (real content length), details 169: base offset and
        // stride must come from lines 1 and 2 respectively, not header alone
        Path f = file(dir, HEADER_UNPADDED + "\n" + DETAIL + "\n" + DETAIL + "\n");
        Map<String, ExecutionContext> parts = new LineRangePartitioner(f.toString()).partition(1);
        assertEquals(1, parts.size());
        ExecutionContext c = parts.get("partition0");
        assertEquals(0, from(c));
        assertEquals(2, to(c));
        assertEquals(169, c.getInt("lrecl"));
        assertEquals(110, c.getLong("baseOffset"));
    }

    @Test
    void nonUtf8ByteDoesNotBreakLengthProbing(@TempDir Path dir) throws IOException {
        // 0xE9 in debtor_name: line lengths are byte counts, never charset decodes
        String detail = "D".repeat(103) + "\u00E9" + "D".repeat(65);
        Path f = file(dir, HEADER_PADDED + "\n" + detail + "\n");
        Map<String, ExecutionContext> parts = new LineRangePartitioner(f.toString()).partition(1);
        assertEquals(1, parts.size());
        assertEquals(1, to(parts.get("partition0")));
        assertEquals(169, parts.get("partition0").getInt("lrecl"));
    }

    @Test
    void raggedFileLengthFailsClosed(@TempDir Path dir) throws IOException {
        // 100 stray bytes: neither a full record nor a newline-less final record
        Path f = file(dir, HEADER_PADDED + "\n" + DETAIL + "\n" + "X".repeat(100));
        LineRangePartitioner partitioner = new LineRangePartitioner(f.toString());
        assertThrows(FileFatalException.class, () -> partitioner.partition(1));
    }

    @Test
    void unknownDetailLreclFailsClosed(@TempDir Path dir) throws IOException {
        Path f = file(dir, HEADER_PADDED + "\n" + "D".repeat(150) + "\n");
        LineRangePartitioner partitioner = new LineRangePartitioner(f.toString());
        assertThrows(FileFatalException.class, () -> partitioner.partition(1));
    }

    @Test
    void splitsEvenlyWithRemainderOnLastPartition() {
        Map<String, ExecutionContext> parts = LineRangePartitioner.split(10, 3);
        assertEquals(3, parts.size());
        assertEquals(0, from(parts.get("partition0")));
        assertEquals(4, to(parts.get("partition0")));   // ceil(10/3)=4
        assertEquals(4, from(parts.get("partition1")));
        assertEquals(8, to(parts.get("partition1")));
        assertEquals(8, from(parts.get("partition2")));
        assertEquals(10, to(parts.get("partition2")));
    }

    @Test
    void fewerRecordsThanPartitionsCollapses() {
        Map<String, ExecutionContext> parts = LineRangePartitioner.split(2, 5);
        assertEquals(2, parts.size());
    }

    @Test
    void zeroRecordsYieldsNoPartitions() {
        assertEquals(0, LineRangePartitioner.split(0, 5).size());
    }
}
