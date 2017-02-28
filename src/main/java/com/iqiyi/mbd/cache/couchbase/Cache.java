package com.iqiyi.mbd.cache.couchbase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface Cache {
    <E extends CacheEntity> Optional<E> get(E entity);

    <E extends CacheEntity<T>,T> Map<T, E> multipleGet(List<T> entityIds, E defaultEntity);

    <E extends CacheEntity> boolean load(E entity);

    <E extends CacheEntity<T>,T> List<T> multipleLoad(List<E> entityList, E defaultEntity);

    <E extends CacheEntity<T>,T> Optional<T> delete(E entity);

    <E extends CacheEntity<T>,T> List<T> multipleDelete(List<T> idList, E defaultEntity);


    void shutDown();
}
