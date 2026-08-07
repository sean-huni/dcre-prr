package za.co.fnb.dcre.prr.data.repo;

import org.springframework.data.repository.CrudRepository;
import za.co.fnb.dcre.prr.data.model.TxEntryEntity;

import java.util.UUID;

/** Read side of tx_entry; ALL writes go through TxEntryBatchDao (single SQL owner). */
public interface TxEntryRepo extends CrudRepository<TxEntryEntity, UUID> {

    long countByArrivalId(UUID arrivalId);
}
