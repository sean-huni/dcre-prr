package za.co.fnb.dcre.prr.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.util.UUID;

/**
 * The payments spine header, in dcre_pay. NO flow column: the database is the
 * family discriminator since the payments split, so a row's presence here IS
 * the statement that it is a payment (PayFlowOnlyTest).
 */
@Table("tx_header")
public class TxHeaderEntity extends BaseEntity {

    private UUID arrivalId;
    private String msgIdRaw;
    private String msgId;
    private String createdTs;
    private Integer txCount;
    private String initgPty;
    private String businessDate;
    private String clientToken;
    private Integer layoutVersion;

    public static TxHeaderEntity of(UUID arrivalId, String msgIdRaw, String msgId, String createdTs,
                                    int txCount, String initgPty, String businessDate,
                                    String clientToken, int layoutVersion) {
        TxHeaderEntity e = new TxHeaderEntity();
        e.arrivalId = arrivalId;
        e.msgIdRaw = msgIdRaw;
        e.msgId = msgId;
        e.createdTs = createdTs;
        e.txCount = txCount;
        e.initgPty = initgPty;
        e.businessDate = businessDate;
        e.clientToken = clientToken;
        e.layoutVersion = layoutVersion;
        return e;
    }

    public UUID getArrivalId() { return arrivalId; }
    public String getMsgIdRaw() { return msgIdRaw; }
    public String getMsgId() { return msgId; }
    public String getCreatedTs() { return createdTs; }
    public Integer getTxCount() { return txCount; }
    public String getInitgPty() { return initgPty; }
    public String getBusinessDate() { return businessDate; }
    public String getClientToken() { return clientToken; }
    public Integer getLayoutVersion() { return layoutVersion; }
}
