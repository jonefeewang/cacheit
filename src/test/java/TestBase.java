import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.example.tutorial.AddressBookProtos;
import com.example.tutorial.AddressBookProtos.PersonProto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;
import dayu.utils.cacheit.BinaryCacheEntity;
import dayu.utils.cacheit.CacheEntity;
import dayu.utils.cacheit.CouchBaseCache;
import dayu.utils.cacheit.CouchBaseException;
import dayu.utils.cacheit.StringCacheEntity;

import lombok.Data;

public class TestBase {
    static CouchBaseCache couchBaseCache;
    static final Gson gson = new Gson();
    static final PersonBean batMan = new PersonBean("The Batman", "101", "batman@qiyi.com");
    static final PersonBean wonderWomen = new PersonBean("Wonder Women", "102", "wonderwomen@qiyi.com");
    static final PersonBean joker = new PersonBean("joker", "201", "joker@qiyi.com");
    static final PersonBean twoFace = new PersonBean("Two-face", "202", "twoface@qiyi.com");
    static final List<PersonBean> heroList = ImmutableList.of(batMan, wonderWomen);
    static final List<PersonBean> villainList = ImmutableList.of(joker, twoFace);
    static final AddressBookBean heroAddressBook = new AddressBookBean("301", heroList);
    static final AddressBookBean villainAddressBook = new AddressBookBean("302", villainList);
    static final List<CacheEntity<String>> generalAddressBookList = ImmutableList.of(heroAddressBook,
                                                                             villainAddressBook);
    static final List<CacheEntity<String>> generalHeroList = ImmutableList.of(batMan, wonderWomen);

    @BeforeClass
    public static void startCodebase() {
        List<InetAddress> cluster1_ip, cluster2_ip;
        List<String> cluster1_buckets, cluster2_buckets;
        Map<String, ImmutablePair<List<InetAddress>, List<String>>> clusterInfo;
        try {
            cluster2_ip = Arrays.asList(InetAddress.getByName("10.153.90.101"),
                                        InetAddress.getByName("10.153.90.102"),
                                        InetAddress.getByName("10.153.90.103"),
                                        InetAddress.getByName("10.153.90.104")
            );
            cluster1_buckets = Arrays.asList("qx_comments");
            cluster2_buckets = Arrays.asList("feed-entity");
            clusterInfo = ImmutableMap.<String, ImmutablePair<List<InetAddress>, List<String>>>builder()
//                    .put("cluster1", ImmutablePair.of(cluster1_ip, cluster1_buckets))
                    .put("cluster2", ImmutablePair.of(cluster2_ip, cluster2_buckets))
                    .build();
            Map<String, Integer> timeoutConfig = new HashMap<>();
            timeoutConfig.put("cluster1_qx_comments", 300);
            timeoutConfig.put("cluster2_feed-entity", 300);
            couchBaseCache = new CouchBaseCache(clusterInfo,timeoutConfig);
        } catch (UnknownHostException e) {
            fail();
        }
    }

    @AfterClass
    public static void shutDownCouchbase() {
        couchBaseCache.shutDown();
    }

    @Data
    public static class PersonBean implements BinaryCacheEntity<String> {
        private final String name;
        private final String id;
        private final String email;
        public static final PersonBean DefaultPersonBean = new PersonBean("", "", "");

        @Override
        public String id() {
            return id;
        }

        @Override
        public int expiry() {
            return 100;
        }

        @Override
        public String keyPrefix() {
            return "PersonBean";
        }

        @Override
        public String db() {
            return "cluster1_qx_comments";
        }

        @Override
        public byte[] encode() {
            return
                    AddressBookProtos.PersonProto.newBuilder().setEmail(email).setName(name).setId(
                            toCacheKey(id))
                                                 .build().toByteArray();
        }

        @Override
        public PersonBean decode(String cacheKey,byte[] content) {
            try {
                PersonProto person = AddressBookProtos.PersonProto.parseFrom(content);
                return new PersonBean(person.getName(), toId(person.getId()), person.getEmail());
            } catch (InvalidProtocolBufferException e) {
                throw new CouchBaseException("unable to decode object form bytes");
            }
        }
    }

    @Data
    public static class AddressBookBean implements StringCacheEntity<String> {
        private final String id;
        private final List<PersonBean> personBeans;

        @Override
        public String id() {
            return id;
        }

        @Override
        public int expiry() {
            return 200;
        }

        @Override
        public String keyPrefix() {
            return "AddressBookBean";
        }

        @Override
        public String db() {
            return "cluster2_feed-entity";
        }

        @Override
        public String encode() {
            return TestBase.gson.toJson(this);
        }

        @Override
        public StringCacheEntity decode(String cacheKey,String content) {
            return TestBase.gson.fromJson(content, AddressBookBean.class);
        }
    }
}
