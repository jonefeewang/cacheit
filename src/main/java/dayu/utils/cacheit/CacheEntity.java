package dayu.utils.cacheit;

public interface CacheEntity<T> {
    T id();

    int expiry();

    String keyPrefix();

    String db();

    default String toCacheKey(String id) {
        return keyPrefix() + "_" + id;
    }

    default T toId(String cacheKey) {
        return (T) cacheKey.substring((keyPrefix() + "_").length());
    }
}
