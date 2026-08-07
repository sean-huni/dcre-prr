package za.co.fnb.dcre.prr.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.prr.service.PaymentRecords;
import za.co.fnb.dcre.prr.service.FileFatalException;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * R-41: splits the detail records of a fixed-width copybook file into
 * contiguous [fromRecord, toRecord) ranges (0-based detail index, header
 * excluded). Byte-exact: line lengths are probed as BYTE counts (never
 * charset decodes), the base offset comes from line 1 (header of any length,
 * padded or not) and the record stride from line 2 (detail LRECL + 1 newline
 * byte). A missing final newline still yields the last record; any other
 * ragged length, or a detail LRECL matching no layout, fails closed.
 */
@Component
@StepScope
public class LineRangePartitioner implements Partitioner {

    static final String FROM_RECORD = "fromRecord";
    static final String TO_RECORD = "toRecord";
    static final String LRECL = "lrecl";
    static final String BASE_OFFSET = "baseOffset";

    private final Path input;

    public LineRangePartitioner(@Value("#{jobParameters['input.file']}") String inputFile) {
        this.input = Path.of(inputFile);
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        try {
            long size = Files.size(input);
            long baseOffset = lineLength(0) + 1L; // header record + its newline
            if (size <= baseOffset) {
                return Map.of(); // header-only file: no detail records
            }
            int lrecl = lineLength(baseOffset);
            // Fail closed on an LRECL matching no layout. UNREACHABLE IN-JOB since
            // SCRUM-107: headerStep runs first and PaymentRecords rejects the same
            // LRECL there, so no job reaches this line with a bad length.
            //
            // Kept deliberately, and the rule that says so, because the same round
            // DELETED an unreachable branch in HeaderService (review I3-4) and
            // consistency was fairly questioned (review N4): delete unreachable code
            // that makes a CLAIM; keep an unreachable fail-closed PRECONDITION at a
            // component boundary. HeaderService's branch chose between two messages
            // where one arm could never be selected, so its javadoc described
            // behaviour prr cannot produce: dead code that lied. This is a precondition
            // on a component that is separately constructed and separately driven by 8
            // unit tests, which is the path that keeps it live. Correcting the round-1
            // report on its own terms: those tests prove the guard through the DIRECT
            // path, not through the job.
            PaymentRecords.detail(lrecl);
            long stride = lrecl + 1L;
            long detailBytes = size - baseOffset;
            long remainder = detailBytes % stride;
            if (remainder != 0 && remainder != lrecl) { // lrecl = final record without newline
                throw new FileFatalException("file length " + size + " is not a whole number of "
                        + lrecl + "-byte records after the " + (baseOffset - 1) + "-byte header");
            }
            long records = Math.ceilDiv(detailBytes, stride);
            Map<String, ExecutionContext> parts = split(records, gridSize);
            parts.values().forEach(context -> {
                context.putInt(LRECL, lrecl);
                context.putLong(BASE_OFFSET, baseOffset);
            });
            return parts;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot partition " + input, e);
        }
    }

    /** Byte count of the line starting at offset, up to \n or EOF; charset-free. */
    private int lineLength(long offset) throws IOException {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(input))) {
            in.skipNBytes(offset);
            int length = 0;
            int b;
            while ((b = in.read()) != -1 && b != '\n') {
                length++;
            }
            return length;
        }
    }

    /** Pure split: chunks of ceil(records/gridSize), remainder on the last partition. */
    static Map<String, ExecutionContext> split(long records, int gridSize) {
        Map<String, ExecutionContext> parts = new LinkedHashMap<>();
        if (records <= 0 || gridSize <= 0) {
            return parts;
        }
        long chunk = (records + gridSize - 1) / gridSize;
        int index = 0;
        for (long from = 0; from < records; from += chunk, index++) {
            ExecutionContext context = new ExecutionContext();
            context.putLong(FROM_RECORD, from);
            context.putLong(TO_RECORD, Math.min(from + chunk, records));
            parts.put("partition" + index, context);
        }
        return parts;
    }
}
