package com.iqiyi.mbd.cache.couchbase;

public interface StringCacheEntity<T> extends CacheEntity<T> {
    String encode();

    StringCacheEntity decode(String content);
}
