package dayu.utils.cacheit;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.util.concurrent.ListenableFuture;

public interface Cache {
    <E extends CacheEntity<T>, T> Optional<E> get(E entity);

    <E extends CacheEntity<T>, T> ListenableFuture<E> getAsync(E entity);

    <E extends CacheEntity<T>, T> Map<T, E> multipleGet(List<T> entityIds, E defaultEntity);

    <E extends CacheEntity<T>, T> boolean load(E entity);

    <E extends CacheEntity<T>, T> ListenableFuture<Boolean> loadAsync(E entity);

    <E extends CacheEntity<T>, T> List<T> multipleLoad(List<E> entityList, E defaultEntity);

    <E extends CacheEntity<T>, T> Optional<T> delete(E entity);

    <E extends CacheEntity<T>, T> ListenableFuture<T> deleteAsync(E entity);

    <E extends CacheEntity<T>, T> List<T> multipleDelete(List<T> idList, E defaultEntity);

    void shutDown();
}
