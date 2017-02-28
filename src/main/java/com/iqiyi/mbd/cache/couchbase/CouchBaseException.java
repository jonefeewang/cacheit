package com.iqiyi.mbd.cache.couchbase;

public class CouchBaseException extends RuntimeException {
    private static final long serialVersionUID = 2061450736047138676L;

    /**
     * Creates a new instance.
     */
    public CouchBaseException() {}

    /**
     * Creates a new instance with the specified {@code message} and {@code cause}.
     */
    public CouchBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance with the specified {@code message}.
     */
    public CouchBaseException(String message) {
        super(message);
    }

    /**
     * Creates a new instance with the specified {@code cause}.
     */
    public CouchBaseException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new instance with the specified {@code message}, {@code cause}, suppression enabled or
     * disabled, and writable stack trace enabled or disabled.
     */
    protected CouchBaseException(String message, Throwable cause, boolean enableSuppression,
                                 boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

