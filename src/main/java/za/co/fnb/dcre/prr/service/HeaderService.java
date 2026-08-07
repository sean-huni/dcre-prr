package za.co.fnb.dcre.prr.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import za.co.fnb.dcre.prr.data.model.TxHeaderEntity;
import za.co.fnb.dcre.prr.data.repo.TxHeaderRepo;
import za.co.fnb.dcre.platform.copybook.CopybookReader;
import za.co.fnb.dcre.platform.copybook.FixedWidthRecord;
import za.co.fnb.dcre.platform.copybook.ShortRecordException;
import za.co.fnb.dcre.platform.files.R31Filename;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

/**
 * Business tier (configuration.md point 21): header parse + the file-fatal
 * structural tier (R-19): length, version incl. V1 fail-closed (A-2),
 * declared count, R-31 filename-vs-header cross-check. Persists via the
 * data/repo tier only.
 *
 * <p>The copybook read itself (ISO_8859_1 byte-transparent decode, fail-closed
 * record-length gate) lives in platform-copybook, shared with the collections
 * and mandates readers so a fix reaches all three instead of one fork.
 *
 * <p>NO flow discriminator, deliberately. CRR stamped tx_header.flow COL or PAY
 * because one reader and one database served both families (SCRUM-69). PRR is
 * the payments reader and writes dcre_pay, so the DATABASE is the discriminator
 * and tx_header has no flow column at all. A flow branch reappearing here would
 * mean the split had not happened, which is what PayFlowOnlyTest enforces.
 */
@Service
public class HeaderService {

    private final TxHeaderRepo repo;
    private final boolean v1Enabled;

    public HeaderService(TxHeaderRepo repo, @Value("${dcre.v1-enabled}") boolean v1Enabled) {
        this.repo = repo;
        this.v1Enabled = v1Enabled;
    }

    /** @return the file-fatal reason, or empty when the header was accepted and persisted. */
    public Optional<String> ingestHeader(final UUID arrivalId, final Path input,
                                        final String originalName) throws IOException {
        try {
            BookRead book = readBook(input);
            if (book.header() == null) {
                throw new FileFatalException("empty file");
            }
            FixedWidthRecord headerRecord = book.header();
            String header = headerRecord.line();
            int version = Integer.parseInt(headerRecord.field("layout_version").strip());
            if (version == 1 && !v1Enabled) {
                throw new FileFatalException("V1 layout fails closed in production (A-2)");
            }
            int declared = Integer.parseInt(headerRecord.field("tx_count").strip());
            int actual = book.detailCount();
            if (declared != actual) {
                throw new FileFatalException("header tx_count=" + declared + " but file has " + actual);
            }
            String destination = headerRecord.field("destination_id").strip();
            Optional<R31Filename.Tokens> tokens = originalName != null
                    ? R31Filename.parse(originalName) : Optional.empty();
            if (tokens.isPresent() && !tokens.get().client().equals(destination)) {
                throw new FileFatalException("R-31 mismatch: filename client " + tokens.get().client()
                        + " != header destination " + destination);
            }
            OpaqueRef msgId = OpaqueRef.ofFixedWidth(header.substring(4, 26));
            repo.upsert(TxHeaderEntity.of(arrivalId, msgId.rawBytes(), msgId.canonical(),
                    headerRecord.field("created_ts"), declared, destination,
                    headerRecord.field("business_date"),
                    tokens.map(R31Filename.Tokens::client).orElse(null), version));
            return Optional.empty();
        } catch (FileFatalException e) {
            return Optional.of(e.getMessage());
        }
    }

    /** Record 0 and the detail count: everything this stage needs from the file. */
    private record BookRead(FixedWidthRecord header, int detailCount) {
    }

    /**
     * The shared copybook read, resolving each record's layout through
     * {@link PaymentRecords} so the header is cut by the header table and a
     * detail by its own. STREAMED: a payment book is large, which is why
     * LineRangePartitioner and FixedRecordRangeReader exist, and this stage
     * needs only record 0 and the count, so it holds one record at a time
     * rather than the whole file plus a wrapper per line.
     *
     * <p>Only record 0 can be short here, so the translation names the header
     * unconditionally: PaymentRecords rejects any detail whose LRECL matches
     * no layout before the short gate is reached, and a detail that DOES match
     * one is exactly that layout's length, so it can never be short. The
     * previous "or report the later record as itself" branch was unreachable.
     */
    private static BookRead readBook(Path input) throws IOException {
        FixedWidthRecord[] header = new FixedWidthRecord[1];
        try {
            long records = CopybookReader.forEachRecord(input, PaymentRecords.INSTANCE,
                    record -> {
                        if (record.index() == 0) {
                            header[0] = record;
                        }
                    });
            return new BookRead(header[0], (int) records - 1);
        } catch (ShortRecordException e) {
            throw new FileFatalException(
                    "header shorter than attested content length " + e.declaredLength());
        }
    }
}
