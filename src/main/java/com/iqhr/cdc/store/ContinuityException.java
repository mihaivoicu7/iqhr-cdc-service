package com.iqhr.cdc.store;

/** Only fixed, non-sensitive diagnostic codes belong in this exception. */
public class ContinuityException extends RuntimeException {
    public ContinuityException(String code) { super(code); }
}
