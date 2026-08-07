package za.co.fnb.dcre.prr.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.math.BigDecimal;
import java.util.UUID;

@Table("tx_entry")
public class TxEntryEntity extends BaseEntity {

    private UUID arrivalId;
    private Integer sequence;
    private String recordType;
    private String e2eRaw;
    private String e2e;
    private String creditorAccount;
    private String contractRef;
    private String currency;
    private String amountRaw;
    private BigDecimal amount;
    private String branchCode;
    private String debtorName;
    private String debtorAccount;
    private String accTypeSeq;
    private String mandateRef;
    private String contentHash;

    public static TxEntryEntity of(UUID arrivalId, int sequence, String recordType, String e2eRaw,
                                   String e2e, String creditorAccount, String contractRef,
                                   String currency, String amountRaw, BigDecimal amount,
                                   String branchCode, String debtorName, String debtorAccount,
                                   String accTypeSeq, String mandateRef, String contentHash) {
        TxEntryEntity e = new TxEntryEntity();
        e.arrivalId = arrivalId;
        e.sequence = sequence;
        e.recordType = recordType;
        e.e2eRaw = e2eRaw;
        e.e2e = e2e;
        e.creditorAccount = creditorAccount;
        e.contractRef = contractRef;
        e.currency = currency;
        e.amountRaw = amountRaw;
        e.amount = amount;
        e.branchCode = branchCode;
        e.debtorName = debtorName;
        e.debtorAccount = debtorAccount;
        e.accTypeSeq = accTypeSeq;
        e.mandateRef = mandateRef;
        e.contentHash = contentHash;
        return e;
    }

    public UUID getArrivalId() { return arrivalId; }
    public Integer getSequence() { return sequence; }
    public String getRecordType() { return recordType; }
    public String getE2eRaw() { return e2eRaw; }
    public String getE2e() { return e2e; }
    public String getCreditorAccount() { return creditorAccount; }
    public String getContractRef() { return contractRef; }
    public String getCurrency() { return currency; }
    public String getAmountRaw() { return amountRaw; }
    public BigDecimal getAmount() { return amount; }
    public String getBranchCode() { return branchCode; }
    public String getDebtorName() { return debtorName; }
    public String getDebtorAccount() { return debtorAccount; }
    public String getAccTypeSeq() { return accTypeSeq; }
    public String getMandateRef() { return mandateRef; }
    public String getContentHash() { return contentHash; }
}
