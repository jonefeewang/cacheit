package dayu.utils.cacheit;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;

public class DefaultCacheFactory implements CacheFactory {
    @Override
    public Cache createCouchBaseCache(
            Map<String, ImmutablePair<List<InetAddress>, List<String>>> clusterBucketMap,Map<String,Integer> timeoutConfig) {
        return new CouchBaseCache(clusterBucketMap,timeoutConfig);
    }
}
