package dayu.utils.cacheit;

public interface BinaryCacheEntity<T> extends CacheEntity<T> {
    byte[] encode();

    BinaryCacheEntity decode(String cacheKey,byte[] content);
}
