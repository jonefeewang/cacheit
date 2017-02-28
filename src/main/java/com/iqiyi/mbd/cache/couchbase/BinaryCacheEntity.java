package com.iqiyi.mbd.cache.couchbase;

public interface BinaryCacheEntity<T> extends CacheEntity<T> {
    byte[] encode();

    BinaryCacheEntity decode(byte[] content);
}
