import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableSet;
import dayu.utils.cacheit.CacheEntity;

@RunWith(Parameterized.class)
public class SingleOpTest extends TestBase {


    @Parameter
    public static CacheEntity<String> entity;

    @Parameters
    public static Collection<CacheEntity<String>> entities(){
        return ImmutableSet.of(batMan,heroAddressBook);
    }


    @Test
    public void singleLoad() {
        assertTrue(couchBaseCache.load(entity));
        clearSample();
    }

    @Test
    public void singleDelete(){
        loadSample();
        Optional<String> result = couchBaseCache.delete(entity);
        assertEquals(Optional.of(entity.id()),result);
    }

    @Test
    public void singleGet() {
        loadSample();
        Optional<CacheEntity<String>> getResult = couchBaseCache.get(entity);
        assertTrue(getResult.isPresent());
        assertTrue(entity.equals(getResult.get()));
        clearSample();
    }
    private void loadSample(){
        assertTrue(couchBaseCache.load(entity));

    }
    private void clearSample(){
        Optional<String> result = couchBaseCache.delete(entity);
        assertEquals(Optional.of(entity.id()),result);

    }


}
