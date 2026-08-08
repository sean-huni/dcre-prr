package za.co.fnb.dcre.prr.service;

/**
 * Structural failure of the whole file: a BUSINESS verdict, never a process death.
 * The NACK is emitted by PIR, the payments responder. CIR is the COLLECTIONS
 * responder and is not on this leg.
 */
public class FileFatalException extends RuntimeException {

    public FileFatalException(String message) {
        super(message);
    }
}
