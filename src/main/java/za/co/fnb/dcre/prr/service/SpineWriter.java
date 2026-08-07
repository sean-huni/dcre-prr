package za.co.fnb.dcre.prr.service;

import za.co.fnb.dcre.prr.data.model.TxEntryEntity;
import za.co.fnb.dcre.prr.data.repo.TxEntryBatchDao;
import za.co.fnb.dcre.platform.copybook.FixedWidthLayout;
import za.co.fnb.dcre.platform.copybook.Layouts;
import za.co.fnb.dcre.platform.model.MoneyText;
import za.co.fnb.dcre.platform.model.OpaqueRef;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * Persists detail lines via the guarded UPSERT keyed (arrival_id, sequence):
 * a restarted chunk rewrites identical rows, never duplicates (R-05); writes
 * are batched per chunk (R-41). Sequence derives from the record's position
 * in the file (recordIndex + 1), never from shared counters, so partitioned
 * ingest is deterministic (R-41). Each row carries a SHA-256 content hash
 * over the essential business fields for CTV's in-file dup scan (R-41).
 * V1 (161) fails closed unless dcre.v1-enabled (A-2). V3 (204) is the V2 body
 * plus a trailing mandate_ref(35), the canonical COLLECTION-to-mandate link
 * (M10). It is mapped here because DETAIL_V3 is one physical layout shared by
 * both families, NOT because a payment can carry a mandate: the mandate gate is
 * DC-only (R-19) and ENDO carries no bank-registered mandates, so an ENDO row's
 * mandate_ref is expected NULL. It stays NULL for V1/V2 books, which have no
 * such field, and is excluded from the content hash either way (it is a link,
 * not part of the money-movement identity).
 */
public class SpineWriter {

    /** A detail line plus its 0-based position among the file's detail records. */
    public record NumberedLine(long recordIndex, String line) {
    }

    private final TxEntryBatchDao dao;
    private final UUID arrivalId;
    private final int amountScale;
    private final boolean v1Enabled;

    public SpineWriter(TxEntryBatchDao dao, UUID arrivalId, int amountScale, boolean v1Enabled) {
        this.dao = dao;
        this.arrivalId = arrivalId;
        this.amountScale = amountScale;
        this.v1Enabled = v1Enabled;
    }

    public void writeDetails(List<? extends NumberedLine> lines) {
        List<TxEntryEntity> entities = new ArrayList<>(lines.size());
        for (NumberedLine numbered : lines) {
            entities.add(toEntity(numbered.line(), (int) numbered.recordIndex() + 1));
        }
        dao.batchUpsert(entities);
    }

    TxEntryEntity toEntity(String line, int seq) {
        FixedWidthLayout layout = layoutFor(line);
        OpaqueRef e2e = OpaqueRef.ofFixedWidth(layout.slice(line, "end_to_end"));
        String amountRaw = layout.slice(line, "amount");
        return TxEntryEntity.of(arrivalId, seq,
                layout.slice(line, "record_type"),
                e2e.rawBytes(), e2e.canonical(),
                layout.slice(line, "creditor_account").strip(),
                layout.slice(line, "contract_ref").strip(),
                layout.slice(line, "currency"),
                amountRaw,
                MoneyText.parse(amountRaw, amountScale),
                layout.slice(line, "branch_code").strip(),
                layout.slice(line, "debtor_name").strip(),
                layout.slice(line, "debtor_account").strip(),
                layout.length() >= Layouts.DETAIL_V2.length()
                        ? layout.slice(line, "acc_type_seq") : null,
                layout.length() == Layouts.DETAIL_V3.length()
                        ? emptyToNull(layout.slice(line, "mandate_ref").strip()) : null,
                contentHash(layout, line));
    }

    /**
     * R-41 content identity: essential business fields only, so a re-keyed
     * copy (fresh e2e, same money movement) still clashes.
     */
    static String contentHash(FixedWidthLayout layout, String line) {
        String essential = String.join("|",
                layout.slice(line, "creditor_account").strip(),
                layout.slice(line, "debtor_account").strip(),
                layout.slice(line, "branch_code").strip(),
                layout.slice(line, "amount"),
                layout.slice(line, "currency"),
                layout.slice(line, "contract_ref").strip());
        return HexFormat.of().formatHex(
                sha256().digest(essential.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** A blank V3 mandate_ref means the record targets no mandate, which is
     *  every ENDO record; store that as NULL so a missing link reads the same as
     *  a V1/V2 row (absent). */
    private static String emptyToNull(String value) {
        return value.isEmpty() ? null : value;
    }

    /** The detail layout for this line, plus the A-2 fail-closed gate. The
     *  SELECTION itself lives in PaymentRecords, shared with the header
     *  pre-read, so the file's record shapes are described in one place. */
    FixedWidthLayout layoutFor(String line) {
        FixedWidthLayout layout = PaymentRecords.detail(line.length());
        if (layout == Layouts.DETAIL_V1 && !v1Enabled) {
            throw new FileFatalException("V1 layout fails closed in production (A-2)");
        }
        return layout;
    }
}
