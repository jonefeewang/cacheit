import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableList;
import com.iqiyi.mbd.cache.couchbase.CacheEntity;

import rx.Observable;

@RunWith(Parameterized.class)
public class MultipleOpTest extends TestBase {
    private List<String> expectedIdList;
    @Parameter
    public static List<CacheEntity<String>> entityList;

    @Parameters
    public static Collection<List<CacheEntity<String>>> entities() {
        return ImmutableList.of(generalHeroList, generalAddressBookList);
    }

    @Before
    public void calcExcepted() {
        expectedIdList = entityList.stream().map(CacheEntity::id).collect(Collectors.toList());
    }

    @Test
    public void multipleLoad() {
        List<String> result = couchBaseCache.multipleLoad(entityList,
                                                          entityList.get(0));
        assertThat(result, containsInAnyOrder(expectedIdList.toArray()));
        clearSample();
    }

    @Test
    public void multipleDelete() {
        loadSample();
        List<String> result = couchBaseCache.multipleDelete(
                entityList.stream().map(CacheEntity::id).collect(
                        Collectors.toList()), entityList.get(0));
        assertThat(result, containsInAnyOrder(expectedIdList.toArray()));
    }

    @Test
    public void multipleGet() {
        loadSample();
        Map<String, CacheEntity<String>> expected = Observable.from(entityList).toMap(CacheEntity::id)
                                                      .toBlocking()
                                                      .single();
        Map<String, CacheEntity<String>> result = couchBaseCache.multipleGet(
                entityList.stream().map(CacheEntity::id).collect(Collectors.toList()),
                entityList.get(0));
        assertThat(result.entrySet(), equalTo(expected.entrySet()));
        clearSample();
    }

    private void clearSample() {
        List<String> result = couchBaseCache.multipleDelete(
                entityList.stream().map(CacheEntity::id).collect(
                        Collectors.toList()), entityList.get(0));
        assertThat(result, containsInAnyOrder(expectedIdList.toArray()));
    }

    private void loadSample() {
        List<String> result = couchBaseCache.multipleLoad(entityList,
                                                          entityList.get(0));
        assertThat(result, containsInAnyOrder(expectedIdList.toArray()));
    }

    @AfterClass
    public static void shutDownCouchbase() {
        couchBaseCache.shutDown();
    }
}
