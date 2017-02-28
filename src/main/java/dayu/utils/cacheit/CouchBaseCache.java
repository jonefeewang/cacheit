package dayu.utils.cacheit;

import static java.util.Objects.requireNonNull;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

import lombok.extern.slf4j.Slf4j;
import rx.Observable;

@Slf4j
public class CouchBaseCache implements Cache {
    private final CouchbaseEnvironment environment;
    private final HashMap<String, CouchbaseCluster> clusterMap;
    private final Map<String, Bucket> bucketMap;

    public CouchBaseCache(Map<String, ImmutablePair<List<InetAddress>, List<String>>> clusterBucketMap) {
        requireNonNull(clusterBucketMap, "clusterBucketMap");
        environment = DefaultCouchbaseEnvironment.create();
        clusterMap = new HashMap<>(clusterBucketMap.size());
        bucketMap = new HashMap<>();
        clusterBucketMap.forEach((k, v) -> {
            CouchbaseCluster cluster = CouchbaseCluster.create(environment, v.getLeft().stream()
                                                                             .map(InetAddress::getHostAddress)
                                                                             .collect(Collectors.toList()));
            v.getRight().forEach(bucket -> bucketMap.put(k + "_" + bucket, cluster.openBucket(bucket)));
            clusterMap.put(k, cluster);
        });
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <E extends CacheEntity> Optional<E> get(E entity) {
        requireNonNull(entity, "entity");
        if (entity instanceof StringCacheEntity) {
            StringCacheEntity stringCacheEntity = (StringCacheEntity) entity;
            return bucketMap.get(entity.db()).async().getAndTouch(
                    entity.toCacheKey(String.valueOf(entity.id())),
                    entity.expiry())
                            .map(
                                    jsonDocument -> Optional
                                            .of(stringCacheEntity.decode(jsonDocument.content().toString())))
                            .toBlocking()
                            .singleOrDefault(Optional.empty()).map(temp -> (E) temp
                    );
        } else if (entity instanceof BinaryCacheEntity) {
            BinaryCacheEntity binaryCacheEntity = (BinaryCacheEntity) entity;
            return bucketMap.get(entity.db()).async().getAndTouch(
                    entity.toCacheKey(String.valueOf(entity.id())),
                    entity.expiry(),
                    BinaryDocument.class).map(
                    binaryDoc -> {
                        Optional<BinaryCacheEntity> binRet = Optional.of(
                                binaryCacheEntity.decode(toBytesArray(binaryDoc.content())));
                        ReferenceCountUtil.release(binaryDoc.content());
                        return binRet;
                    }
            )
                            .toBlocking()
                            .singleOrDefault(Optional.empty()).map(temp -> (E) temp
                    );
        } else { throw new CouchBaseException("unsupported subtype:" + entity.getClass()); }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public <E extends CacheEntity<T>, T> Map<T, E> multipleGet(List<T> entityIds, E entity) {
        requireNonNull(entityIds, "entityIds");
        requireNonNull(entity, "entity");
        if (entity instanceof BinaryCacheEntity) {
            return Observable
                    .from(entityIds).map(entityId -> entity.toCacheKey(String.valueOf(entityId)))
                    .flatMap(id -> bucketMap.get(entity.db()).async()
                                            .getAndTouch(String.valueOf(id), entity.expiry(),
                                                         BinaryDocument.class)
                                            .doOnError(throwable -> log.warn(throwable.getMessage()))
                                            .onErrorResumeNext(Observable.empty())
                    )
                    .map(binaryDocument -> {
                        BinaryCacheEntity temp = (BinaryCacheEntity) entity;
                        BinaryCacheEntity decoded = temp.decode(toBytesArray(binaryDocument.content()));
                        ReferenceCountUtil.release(binaryDocument.content());
                        return (E) decoded;
                    })
                    .toMap(CacheEntity::id)
                    .toBlocking()
                    .single();
        } else if (entity instanceof StringCacheEntity) {
            return Observable
                    .from(entityIds).map(entityId -> entity.toCacheKey(String.valueOf(entityId)))
                    .flatMap(id -> bucketMap.get(entity.db()).async()
                                            .getAndTouch(String.valueOf(id), entity.expiry())
                                            .doOnError(throwable -> log.warn(throwable.getMessage()))
                                            .onErrorResumeNext(Observable.empty())
                    )
                    .map(jsonDocument -> {
                        StringCacheEntity temp = (StringCacheEntity) entity;
                        StringCacheEntity decoded = temp.decode(jsonDocument.content().toString());
                        return (E) decoded;
                    })
                    .toMap(CacheEntity::id)
                    .toBlocking()
                    .single();
        } else { throw new CouchBaseException("unsupported subtype:" + entity.getClass()); }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean load(CacheEntity entity) {
        requireNonNull(entity, "entity");
        if (entity instanceof StringCacheEntity) {
            RawJsonDocument result = bucketMap.get(entity.db()).async().upsert(
                    RawJsonDocument.create(entity.toCacheKey(String.valueOf(entity.id())), entity.expiry(),
                                           ((StringCacheEntity) entity)
                                                   .encode())).toBlocking().singleOrDefault(null);
            if (result == null) { return false; }
            return true;
        } else if (entity instanceof BinaryCacheEntity) {
            BinaryDocument result = bucketMap.get(entity.db()).async().upsert(
                    BinaryDocument.create(entity.toCacheKey(String.valueOf(entity.id())), entity.expiry(),
                                          Unpooled.wrappedBuffer(
                                                  ((BinaryCacheEntity) entity)
                                                          .encode()))).toBlocking().singleOrDefault(null);
            if (result == null) { return false; }
            return true;
        } else {
            throw new CouchBaseException("unsupported subtype:" + entity.getClass());
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <E extends CacheEntity<T>, T> List<T> multipleLoad(List<E> entityList, E defaultEntity) {
        requireNonNull(defaultEntity, "defaultEntity");
        requireNonNull(entityList, "entityList");
        if (defaultEntity instanceof StringCacheEntity) {
            return Observable.from(entityList).flatMap(
                    entity -> bucketMap.get(defaultEntity.db()).async()
                                       .upsert(RawJsonDocument
                                                       .create(entity.toCacheKey(String.valueOf(entity.id())),
                                                               ((StringCacheEntity) entity)
                                                                       .encode(),
                                                               defaultEntity
                                                                       .expiry()))
                                       .doOnError(throwable -> log.warn(throwable.getMessage()))
                                       .onErrorResumeNext(Observable.empty())
            )
                             .map(json -> defaultEntity.toRealKey(json.id())).toList().toBlocking()
                             .singleOrDefault(Collections.emptyList());
        } else if (defaultEntity instanceof BinaryCacheEntity) {
            return Observable.from(entityList).flatMap(
                    entity -> bucketMap.get(defaultEntity.db()).async()
                                       .upsert(BinaryDocument
                                                       .create(entity.toCacheKey(String.valueOf(entity.id())),
                                                               entity.expiry(),
                                                               Unpooled.wrappedBuffer(
                                                                       ((BinaryCacheEntity) entity)
                                                                               .encode()),
                                                               defaultEntity
                                                                       .expiry()))
                                       .doOnError(throwable -> log.warn(throwable.getMessage()))
                                       .onErrorResumeNext(Observable.empty())
            )
                             .map(json -> defaultEntity.toRealKey(json.id())).toList().toBlocking()
                             .singleOrDefault(Collections.emptyList());
        }
        throw new CouchBaseException("unsupported subtype:" + entityList.getClass());
    }

    @Override
    public <E extends CacheEntity<T>, T> Optional<T> delete(E entity) {
        requireNonNull(entity, "entity");
        return bucketMap.get(entity.db()).async().remove(entity.toCacheKey(String.valueOf(entity.id())))
                        .map(
                                jsonDocument -> Optional
                                        .of(entity.toRealKey(jsonDocument.id()))).toBlocking()
                        .singleOrDefault(Optional.empty());
    }

    @Override
    public <E extends CacheEntity<T>, T> List<T> multipleDelete(List<T> idList, E defaultEntity) {
        requireNonNull(defaultEntity, "defaultEntity");
        requireNonNull(idList, "idList");
        return Observable.from(idList).flatMap(id -> bucketMap.get(defaultEntity.db()).async()
                                                              .remove(defaultEntity
                                                                              .toCacheKey(String.valueOf(id)))
                                                              .doOnError(throwable -> log
                                                                      .warn(throwable.getMessage()))
                                                              .onErrorResumeNext(Observable.empty())
        )
                         .map(
                                 jsonDocument -> defaultEntity.toRealKey(jsonDocument.id())).toList()
                         .toBlocking()
                         .singleOrDefault(
                                 Collections.emptyList());
    }

    @Override
    public void shutDown() {
        Observable
                .from(clusterMap.values())
                .map(CouchbaseCluster::disconnect)
                .last()
                .map(aBoolean -> environment.shutdown())
                .toBlocking()
                .single();
    }

    private static byte[] toBytesArray(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return bytes;

        /*
           //todo: use the following implementation to reduce memory copy when PB library support. wangjf/2017/02/22
        byte[] bytes;
            int offset;
            int length = buf.readableBytes();

            if (buf.hasArray()) {
                bytes = buf.array();
                offset = buf.arrayOffset();
            } else {
                bytes = new byte[length];
                buf.getBytes(buf.readerIndex(), bytes);
                offset = 0;
            }*/
    }
}
