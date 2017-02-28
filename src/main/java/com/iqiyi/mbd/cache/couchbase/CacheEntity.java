package com.iqiyi.mbd.cache.couchbase;

public interface CacheEntity<T> {
    T id();

    int expiry();

    String keyPrefix();

    String db();

    default String toCacheKey(String id) {
        return keyPrefix() + "_" + id;
    }

    default T toRealKey(String cacheKey) {
        return (T) cacheKey.substring((keyPrefix() + "_").length());
    }
}
