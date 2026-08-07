package za.co.fnb.dcre.prr.batch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import za.co.fnb.dcre.prr.service.FileFatalException;
import za.co.fnb.dcre.prr.service.SpineWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Byte-offset range reader: layout validation lives in the partitioner, so
 * these fixtures use a compact LRECL 10 with a 10-byte header (base offset 11).
 */
class FixedRecordRangeReaderTest {

    private static final int LRECL = 10;
    private static final long BASE_OFFSET = 11; // 10-byte header + newline

    private static Path file(Path dir, String content) throws IOException {
        Path f = dir.resolve("records.txt");
        Files.writeString(f, content, StandardCharsets.ISO_8859_1);
        return f;
    }

    private static FixedRecordRangeReader reader(Path f, long from, long to) {
        return new FixedRecordRangeReader(f.toString(), from, to, LRECL, BASE_OFFSET);
    }

    private static final String HEADER = "HHHHHHHHHH";
    private static final String R0 = "RECORD-000";
    private static final String R1 = "RECORD-111";
    private static final String R2 = "RECORD-222";
    private static final String R3 = "RECORD-333";

    @Test
    void freshOpenReadsItsRangeOnly(@TempDir Path dir) throws Exception {
        Path f = file(dir, String.join("\n", HEADER, R0, R1, R2, R3) + "\n");
        FixedRecordRangeReader r = reader(f, 1, 3);
        r.open(new ExecutionContext());
        assertEquals(new SpineWriter.NumberedLine(1, R1), r.read());
        assertEquals(new SpineWriter.NumberedLine(2, R2), r.read());
        assertNull(r.read(), "range end must stop before record 3");
        r.close();
    }

    @Test
    void restartResumesMidRangeFromReadCount(@TempDir Path dir) throws Exception {
        Path f = file(dir, String.join("\n", HEADER, R0, R1, R2, R3) + "\n");
        ExecutionContext resumed = new ExecutionContext();
        resumed.putLong("read.count", 2); // R-05: two records already written
        FixedRecordRangeReader r = reader(f, 0, 4);
        r.open(resumed);
        assertEquals(new SpineWriter.NumberedLine(2, R2), r.read());
        ExecutionContext saved = new ExecutionContext();
        r.update(saved);
        assertEquals(3, saved.getLong("read.count"));
        r.close();
    }

    @Test
    void lastRecordWithoutTrailingNewlineIsRead(@TempDir Path dir) throws Exception {
        Path f = file(dir, String.join("\n", HEADER, R0, R1)); // no final newline
        FixedRecordRangeReader r = reader(f, 0, 2);
        r.open(new ExecutionContext());
        assertEquals(new SpineWriter.NumberedLine(0, R0), r.read());
        assertEquals(new SpineWriter.NumberedLine(1, R1), r.read());
        assertNull(r.read());
        r.close();
    }

    @Test
    void wrongSeparatorByteFailsClosedNamingTheRecord(@TempDir Path dir) throws Exception {
        // record 1 is one byte too long: the byte where its newline belongs is 'X'
        Path f = file(dir, HEADER + "\n" + R0 + "\n" + R1 + "X" + "\n" + R2 + "\n");
        FixedRecordRangeReader r = reader(f, 0, 3);
        r.open(new ExecutionContext());
        r.read(); // record 0 fine
        FileFatalException thrown = assertThrows(FileFatalException.class, r::read);
        assertTrue(thrown.getMessage().contains("record 1"), "unexpected: " + thrown.getMessage());
        r.close();
    }

    @Test
    void nonUtf8ByteDecodesByteTransparently(@TempDir Path dir) throws Exception {
        // 0xE9 within the record: ISO_8859_1 keeps it as U+00E9, one byte = one char
        Path f = file(dir, HEADER + "\n" + "REC\u00E9RD-000" + "\n");
        FixedRecordRangeReader r = reader(f, 0, 1);
        r.open(new ExecutionContext());
        SpineWriter.NumberedLine line = r.read();
        assertEquals(LRECL, line.line().length());
        assertEquals('\u00E9', line.line().charAt(3));
        r.close();
    }
}
