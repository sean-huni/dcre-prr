package za.co.fnb.dcre.prr.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.prr.data.model.TxHeaderEntity;

import java.util.UUID;

/** Guarded native UPSERT keyed by the R-05 identity (arrival_id): restarts rewrite, never duplicate. */
public interface TxHeaderRepo extends CrudRepository<TxHeaderEntity, UUID> {

    @Modifying
    @Query("""
            INSERT INTO tx_header (arrival_id, msg_id_raw, msg_id, created_ts, tx_count, initg_pty, business_date, client_token, layout_version)
            VALUES (:#{#e.arrivalId}, :#{#e.msgIdRaw}, :#{#e.msgId}, :#{#e.createdTs}, :#{#e.txCount}, :#{#e.initgPty}, :#{#e.businessDate}, :#{#e.clientToken}, :#{#e.layoutVersion})
            ON CONFLICT (arrival_id) DO UPDATE SET msg_id_raw = EXCLUDED.msg_id_raw, msg_id = EXCLUDED.msg_id, created_ts = EXCLUDED.created_ts, tx_count = EXCLUDED.tx_count, initg_pty = EXCLUDED.initg_pty, business_date = EXCLUDED.business_date, client_token = EXCLUDED.client_token, layout_version = EXCLUDED.layout_version""")
    void upsert(@Param("e") TxHeaderEntity e);
}
