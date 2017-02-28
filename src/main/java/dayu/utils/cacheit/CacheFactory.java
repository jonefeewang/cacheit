package dayu.utils.cacheit;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;

public interface CacheFactory {
     Cache createCouchBaseCache(Map<String, ImmutablePair<List<InetAddress>, List<String>>> clusterBucketMap);
}
