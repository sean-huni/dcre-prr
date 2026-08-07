package za.co.fnb.dcre.prr.service;

/** Structural failure of the whole file: a BUSINESS verdict (NACK via CIR), never a process death. */
public class FileFatalException extends RuntimeException {

    public FileFatalException(String message) {
        super(message);
    }
}
