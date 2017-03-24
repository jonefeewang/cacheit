package dayu.utils.cacheit;

public interface StringCacheEntity<T> extends CacheEntity<T> {
    String encode();

    StringCacheEntity decode(String cacheKey,String content);
}
