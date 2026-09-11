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

    /** The launch parameter naming the file to partition; AGT supplies it on every launch. */
    static final String INPUT_FILE = "input.file";

    private final Path input;

    public LineRangePartitioner(@Value("#{jobParameters['" + INPUT_FILE + "']}") String inputFile) {
        this.input = Path.of(requiredInputFile(inputFile));
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

    /**
     * Names the missing parameter, and this stage, before the value can reach {@link Path#of}.
     * Unguarded, a launch without {@code input.file} dies inside the JDK's filesystem code with a
     * NullPointerException that names neither the parameter, nor this stage, nor the job, so the
     * operator reading that log learns nothing about what to supply. Blank counts as missing: a
     * blank string builds an empty path with no NullPointerException at all and then fails
     * somewhere else entirely, which is the quieter half of the same defect.
     *
     * <p>Deliberately NOT a {@code FileFatalException}: that type is a BUSINESS verdict about the
     * file's contents and is routed to a NACK. A parameter the launch never supplied is an
     * operator error about the launch, and there is no file to pass judgement on.
     */
    private static String requiredInputFile(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("PRR requires the job parameter '" + INPUT_FILE
                    + "': it was " + (value == null ? "not supplied" : "blank ('" + value + "')")
                    + ". Launch the job with " + INPUT_FILE + "=<path to the file to partition>.");
        }
        return value;
    }
}
