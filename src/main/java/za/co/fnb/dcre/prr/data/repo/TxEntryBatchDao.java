package za.co.fnb.dcre.prr.data.repo;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import za.co.fnb.dcre.prr.data.model.TxEntryEntity;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Sole owner of the tx_entry write SQL: the guarded UPSERT keyed
 * (arrival_id, sequence) (R-05), driven through JdbcTemplate.batchUpdate in
 * 500-row batches so a 50k-tx file costs ~100 round trips, not 50k (R-41).
 */
@Component
public class TxEntryBatchDao {

    static final int BATCH_SIZE = 500;

    private static final String UPSERT_SQL = """
            INSERT INTO tx_entry (arrival_id, sequence, record_type, e2e_raw, e2e, creditor_account, contract_ref, currency, amount_raw, amount, branch_code, debtor_name, debtor_account, acc_type_seq, mandate_ref, content_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (arrival_id, sequence) DO UPDATE SET record_type = EXCLUDED.record_type, e2e_raw = EXCLUDED.e2e_raw, e2e = EXCLUDED.e2e, creditor_account = EXCLUDED.creditor_account, contract_ref = EXCLUDED.contract_ref, currency = EXCLUDED.currency, amount_raw = EXCLUDED.amount_raw, amount = EXCLUDED.amount, branch_code = EXCLUDED.branch_code, debtor_name = EXCLUDED.debtor_name, debtor_account = EXCLUDED.debtor_account, acc_type_seq = EXCLUDED.acc_type_seq, mandate_ref = EXCLUDED.mandate_ref, content_hash = EXCLUDED.content_hash""";

    private final JdbcTemplate jdbc;

    public TxEntryBatchDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void batchUpsert(List<TxEntryEntity> entities) {
        if (entities.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(UPSERT_SQL, entities, BATCH_SIZE, TxEntryBatchDao::bind);
    }

    private static void bind(PreparedStatement ps, TxEntryEntity e) throws SQLException {
        ps.setObject(1, e.getArrivalId());
        ps.setInt(2, e.getSequence());
        ps.setString(3, e.getRecordType());
        ps.setString(4, e.getE2eRaw());
        ps.setString(5, e.getE2e());
        ps.setString(6, e.getCreditorAccount());
        ps.setString(7, e.getContractRef());
        ps.setString(8, e.getCurrency());
        ps.setString(9, e.getAmountRaw());
        ps.setBigDecimal(10, e.getAmount());
        ps.setString(11, e.getBranchCode());
        ps.setString(12, e.getDebtorName());
        ps.setString(13, e.getDebtorAccount());
        ps.setString(14, e.getAccTypeSeq());
        ps.setString(15, e.getMandateRef());
        ps.setString(16, e.getContentHash());
    }
}
