package za.co.fnb.dcre.prr.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.prr.service.FileFatalException;
import za.co.fnb.dcre.prr.service.SpineWriter;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * Reads one partition's detail records [fromRecord, toRecord) by byte offset:
 * record n starts at baseOffset + n * (lrecl + 1). Decoding is ISO_8859_1,
 * byte-transparent: one byte = one char, so offsets and slice positions never
 * drift on non-UTF-8 bytes. Every record must end in a newline (EOF accepted
 * for the file's final record); anything else fails closed naming the record.
 * Restart-safe per R-05: records already read are stored under read.count so
 * a resumed partition seeks past them.
 */
@Component
@StepScope
public class FixedRecordRangeReader implements ItemStreamReader<SpineWriter.NumberedLine> {

    static final String READ_COUNT = "read.count";

    private final String inputFile;
    private final long fromRecord;
    private final long toRecord;
    private final int lrecl;
    private final long baseOffset;

    private RandomAccessFile file;
    private long recordsRead;

    public FixedRecordRangeReader(
            @Value("#{jobParameters['input.file']}") String inputFile,
            @Value("#{stepExecutionContext['fromRecord']}") long fromRecord,
            @Value("#{stepExecutionContext['toRecord']}") long toRecord,
            @Value("#{stepExecutionContext['lrecl']}") int lrecl,
            @Value("#{stepExecutionContext['baseOffset']}") long baseOffset) {
        this.inputFile = inputFile;
        this.fromRecord = fromRecord;
        this.toRecord = toRecord;
        this.lrecl = lrecl;
        this.baseOffset = baseOffset;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        recordsRead = executionContext.containsKey(READ_COUNT)
                ? executionContext.getLong(READ_COUNT) : 0L;
        try {
            file = new RandomAccessFile(inputFile, "r");
            file.seek(baseOffset + (fromRecord + recordsRead) * (lrecl + 1L));
        } catch (IOException e) {
            throw new ItemStreamException("cannot open " + inputFile, e);
        }
    }

    @Override
    public SpineWriter.NumberedLine read() throws IOException {
        if (recordsRead >= toRecord - fromRecord) {
            return null;
        }
        byte[] buffer = new byte[lrecl];
        file.readFully(buffer);
        long recordIndex = fromRecord + recordsRead;
        int separator = file.read();
        if (separator != '\n' && separator != -1) {
            throw new FileFatalException("record " + recordIndex + " overruns its " + lrecl
                    + "-byte layout: separator byte 0x" + Integer.toHexString(separator)
                    + " is not a newline");
        }
        recordsRead++;
        return new SpineWriter.NumberedLine(recordIndex, new String(buffer, StandardCharsets.ISO_8859_1));
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putLong(READ_COUNT, recordsRead);
    }

    @Override
    public void close() throws ItemStreamException {
        if (file == null) {
            return;
        }
        try {
            file.close();
        } catch (IOException e) {
            throw new ItemStreamException("cannot close " + inputFile, e);
        } finally {
            file = null;
        }
    }
}
