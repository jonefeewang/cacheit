package dayu.utils.cacheit;

import static java.util.Objects.requireNonNull;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import lombok.extern.slf4j.Slf4j;
import rx.Observable;
import rx.observable.ListenableFutureObservable;

@Slf4j
public class CouchBaseCache implements Cache {
    private final CouchbaseEnvironment environment;
    private final HashMap<String, CouchbaseCluster> clusterMap;
    private final Map<String, Bucket> bucketMap;
    private final Map<String, Integer> timeoutConfig;

    public CouchBaseCache(Map<String, ImmutablePair<List<InetAddress>, List<String>>> clusterBucketMap,
                          Map<String, Integer> timeoutConfig) {
        requireNonNull(clusterBucketMap, "clusterBucketMap");
        environment = DefaultCouchbaseEnvironment.create();
        clusterMap = new HashMap<>(clusterBucketMap.size());
        this.timeoutConfig = timeoutConfig;
        bucketMap = new HashMap<>();
        clusterBucketMap.forEach((k, v) -> {
            CouchbaseCluster cluster = CouchbaseCluster.create(environment, v.getLeft().stream()
                                                                             .map(InetAddress::getHostAddress)
                                                                             .collect(Collectors.toList()));
            v.getRight().forEach(bucket -> bucketMap.put(k + "_" + bucket, cluster.openBucket(bucket)));
            clusterMap.put(k, cluster);
        });
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <E extends CacheEntity<T>, T> Optional<E> get(E entity) {
        requireNonNull(entity, "entity");
        if (entity instanceof StringCacheEntity) {
            StringCacheEntity<T> stringCacheEntity = (StringCacheEntity<T>) entity;
            return bucketMap.get(entity.db()).async().getAndTouch(
                    entity.toCacheKey(String.valueOf(entity.id())),
                    entity.expiry(), RawJsonDocument.class).timeout(timeoutConfig.get(entity.db()),
                                                                    TimeUnit.MILLISECONDS)
                            .map(
                                    jsonDocument -> Optional
                                            .of(stringCacheEntity
                                                        .decode(jsonDocument.id(), jsonDocument.content())))
                            .toBlocking()
                            .singleOrDefault(Optional.empty()).map(temp -> (E) temp
                    );
        } else if (entity instanceof BinaryCacheEntity) {
            BinaryCacheEntity<T> binaryCacheEntity = (BinaryCacheEntity<T>) entity;
            return bucketMap.get(entity.db()).async().getAndTouch(
                    entity.toCacheKey(String.valueOf(entity.id())),
                    entity.expiry(),
                    BinaryDocument.class).timeout(timeoutConfig.get(entity.db()), TimeUnit.MILLISECONDS).map(
                    binaryDoc -> {
                        Optional<BinaryCacheEntity<T>> binRet = Optional.of(
                                binaryCacheEntity.decode(binaryDoc.id(), toBytesArray(binaryDoc.content())));
                        ReferenceCountUtil.release(binaryDoc.content());
                        return binRet;
                    }
            )
                            .toBlocking()
                            .singleOrDefault(Optional.empty()).map(temp -> (E) temp
                    );
        } else { throw new CouchBaseException("unsupported subtype:" + entity.getClass()); }
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <E extends CacheEntity<T>, T> ListenableFuture<E> getAsync(E entity) {
        requireNonNull(entity, "entity");
        if (entity instanceof StringCacheEntity) {
            StringCacheEntity<T> stringCacheEntity = (StringCacheEntity<T>) entity;
            return ListenableFutureObservable.to(bucketMap.get(entity.db()).async().getAndTouch(
                    entity.toCacheKey(String.valueOf(entity.id())),
                    entity.expiry(), RawJsonDocument.class).timeout(timeoutConfig.get(entity.db()),
                                                                    TimeUnit.MILLISECONDS)
                                                          .map(jsonDocument -> (E) stringCacheEntity
                                                                  .decode(jsonDocument.id(),
                                                                          jsonDocument.content())));
        } else if (entity instanceof BinaryCacheEntity) {
            BinaryCacheEntity<T> binaryCacheEntity = (BinaryCacheEntity<T>) entity;
            return ListenableFutureObservable.to(bucketMap.get(entity.db()).async().getAndTouch(
                    entity.toCacheKey(String.valueOf(entity.id())),
                    entity.expiry(),
                    BinaryDocument.class).timeout(timeoutConfig.get(entity.db()), TimeUnit.MILLISECONDS).map(
                    binaryDoc -> {
                        BinaryCacheEntity<T> binRet =
                                binaryCacheEntity.decode(binaryDoc.id(), toBytesArray(binaryDoc.content()));
                        ReferenceCountUtil.release(binaryDoc.content());
                        return (E) binRet;
                    }
            ));
        } else { throw new CouchBaseException("unsupported subtype:" + entity.getClass()); }
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <E extends CacheEntity<T>, T> Map<T, E> multipleGet(List<T> entityIds, E entity) {
        requireNonNull(entityIds, "entityIds");
        requireNonNull(entity, "entity");
        if (entity instanceof BinaryCacheEntity) {
            return Observable
                    .from(entityIds).map(entityId -> entity.toCacheKey(String.valueOf(entityId)))
                    .flatMap(id -> bucketMap.get(entity.db()).async()
                                            .getAndTouch(String.valueOf(id), entity.expiry(),
                                                         BinaryDocument.class).timeout(
                                    timeoutConfig.get(entity.db()), TimeUnit.MILLISECONDS)
                                            .doOnError(throwable -> log.warn(throwable.getMessage()))
                                            .onErrorResumeNext(Observable.empty())
                    )
                    .map(binaryDocument -> {
                        BinaryCacheEntity<T> temp = (BinaryCacheEntity<T>) entity;
                        BinaryCacheEntity<T> decoded = temp.decode(binaryDocument.id(),
                                                                   toBytesArray(binaryDocument.content()));
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
                                            .getAndTouch(String.valueOf(id), entity.expiry(),
                                                         RawJsonDocument.class).timeout(
                                    timeoutConfig.get(entity.db()), TimeUnit.MILLISECONDS)
                                            .doOnError(throwable -> log.warn(throwable.getMessage()))
                                            .onErrorResumeNext(Observable.empty())
                    )
                    .map(jsonDocument -> {
                        StringCacheEntity<T> temp = (StringCacheEntity<T>) entity;
                        StringCacheEntity<T> decoded = temp.decode(jsonDocument.id(), jsonDocument.content());
                        return (E) decoded;
                    })
                    .toMap(CacheEntity::id)
                    .toBlocking()
                    .single();
        } else { throw new CouchBaseException("unsupported subtype:" + entity.getClass()); }
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public <E extends CacheEntity<T>, T> boolean load(E entity) {
        requireNonNull(entity, "entity");
        if (entity instanceof StringCacheEntity) {
            RawJsonDocument result = bucketMap.get(entity.db()).async().upsert(
                    RawJsonDocument.create(entity.toCacheKey(String.valueOf(entity.id())), entity.expiry(),
                                           ((StringCacheEntity<T>) entity)
                                                   .encode())).timeout(timeoutConfig.get(entity.db()),
                                                                       TimeUnit.MILLISECONDS).toBlocking()
                                              .singleOrDefault(null);
            if (result == null) { return false; }
            return true;
        } else if (entity instanceof BinaryCacheEntity) {
            BinaryDocument result = bucketMap.get(entity.db()).async().upsert(
                    BinaryDocument.create(entity.toCacheKey(String.valueOf(entity.id())), entity.expiry(),
                                          Unpooled.wrappedBuffer(
                                                  ((BinaryCacheEntity<T>) entity)
                                                          .encode()))).timeout(timeoutConfig.get(entity.db()),
                                                                               TimeUnit.MILLISECONDS)
                                             .toBlocking().singleOrDefault(null);
            if (result == null) { return false; }
            return true;
        } else {
            throw new CouchBaseException("unsupported subtype:" + entity.getClass());
        }
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public <E extends CacheEntity<T>, T> ListenableFuture<Boolean> loadAsync(E entity) {
        requireNonNull(entity, "entity");
        if (entity instanceof StringCacheEntity) {
            return Futures.transform(ListenableFutureObservable.to(bucketMap.get(entity.db()).async()
                                                                            .upsert(RawJsonDocument
                                                                                            .create(entity.toCacheKey(
                                                                                                    String.valueOf(
                                                                                                            entity.id())),
                                                                                                    entity.expiry(),
                                                                                                    ((StringCacheEntity<T>) entity)
                                                                                                            .encode()))
                                                                            .timeout(timeoutConfig
                                                                                             .get(entity.db()),
                                                                                     TimeUnit.MILLISECONDS)),
                                     input -> true);
        } else if (entity instanceof BinaryCacheEntity) {
            return Futures.transform(ListenableFutureObservable
                                             .to(bucketMap.get(entity.db()).async().upsert(
                                                     BinaryDocument
                                                             .create(entity.toCacheKey(
                                                                     String.valueOf(entity.id())),
                                                                     entity.expiry(),
                                                                     Unpooled.wrappedBuffer(
                                                                             ((BinaryCacheEntity<T>) entity)
                                                                                     .encode())))
                                                          .timeout(timeoutConfig.get(entity.db()),
                                                                   TimeUnit.MILLISECONDS)),
                                     input -> true);
        } else {
            throw new CouchBaseException("unsupported subtype:" + entity.getClass());
        }
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <E extends CacheEntity<T>, T> List<T> multipleLoad(List<E> entityList, E defaultEntity) {
        requireNonNull(defaultEntity, "defaultEntity");
        requireNonNull(entityList, "entityList");
        if (defaultEntity instanceof StringCacheEntity) {
            return Observable.timer(300, TimeUnit.MILLISECONDS).from(entityList).flatMap(
                    entity -> bucketMap.get(defaultEntity.db()).async()
                                       .upsert(RawJsonDocument
                                                       .create(entity.toCacheKey(String.valueOf(entity.id())),
                                                               ((StringCacheEntity<T>) entity)
                                                                       .encode(),
                                                               defaultEntity
                                                                       .expiry())).timeout(
                                    timeoutConfig.get(defaultEntity.db()), TimeUnit.MILLISECONDS)
                                       .doOnError(throwable -> log.warn(throwable.getMessage()))
                                       .onErrorResumeNext(Observable.empty())
            )
                             .map(json -> defaultEntity.toId(json.id())).toList().toBlocking()
                             .singleOrDefault(Collections.emptyList());
        } else if (defaultEntity instanceof BinaryCacheEntity) {
            return Observable.from(entityList).flatMap(
                    entity -> bucketMap.get(defaultEntity.db()).async()
                                       .upsert(BinaryDocument
                                                       .create(entity.toCacheKey(String.valueOf(entity.id())),
                                                               entity.expiry(),
                                                               Unpooled.wrappedBuffer(
                                                                       ((BinaryCacheEntity<T>) entity)
                                                                               .encode()),
                                                               defaultEntity
                                                                       .expiry())).timeout(
                                    timeoutConfig.get(defaultEntity.db()), TimeUnit.MILLISECONDS)
                                       .doOnError(throwable -> log.warn(throwable.getMessage()))
                                       .onErrorResumeNext(Observable.empty())
            )
                             .map(json -> defaultEntity.toId(json.id())).toList().toBlocking()
                             .singleOrDefault(Collections.emptyList());
        }
        throw new CouchBaseException("unsupported subtype:" + entityList.getClass());
    }

    @Override
    public <E extends CacheEntity<T>, T> Optional<T> delete(E entity) {
        requireNonNull(entity, "entity");
        return bucketMap.get(entity.db()).async().remove(entity.toCacheKey(String.valueOf(entity.id())))
                        .timeout(timeoutConfig.get(entity.db()), TimeUnit.MILLISECONDS)
                        .map(
                                jsonDocument -> Optional
                                        .of(entity.toId(jsonDocument.id()))).toBlocking()
                        .singleOrDefault(Optional.empty());
    }

    @Override
    public <E extends CacheEntity<T>, T> ListenableFuture<T> deleteAsync(E entity) {
        return ListenableFutureObservable.to(bucketMap.get(entity.db()).async()
                                                      .remove(entity.toCacheKey(String.valueOf(entity.id())))
                                                      .timeout(timeoutConfig.get(entity.db()),
                                                               TimeUnit.MILLISECONDS)
                                                      .map(jsonDocument -> entity
                                                              .toId(jsonDocument.id())));
    }

    @Override
    public <E extends CacheEntity<T>, T> List<T> multipleDelete(List<T> idList, E defaultEntity) {
        requireNonNull(defaultEntity, "defaultEntity");
        requireNonNull(idList, "idList");
        return Observable.from(idList).flatMap(id -> bucketMap.get(defaultEntity.db()).async()
                                                              .remove(defaultEntity
                                                                              .toCacheKey(String.valueOf(id)))
                                                              .timeout(timeoutConfig.get(defaultEntity.db()),
                                                                       TimeUnit.MILLISECONDS)
                                                              .doOnError(throwable -> log
                                                                      .warn(throwable.getMessage()))
                                                              .onErrorResumeNext(Observable.empty())
        )
                         .map(
                                 jsonDocument -> defaultEntity.toId(jsonDocument.id())).toList()
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
