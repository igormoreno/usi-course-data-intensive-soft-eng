import ch.usi.dataIntensiveSoftEng.polystore.MongoDBStore;
import ch.usi.dataIntensiveSoftEng.polystore.RedisStore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ConnectionTest {
    @Test
    public void testRedisStoreConnection() {
        RedisStore redisStore = new RedisStore("hydra.unamurcs.be", 63794);
        assertTrue(redisStore.getProduct(52).size() > 0);
    }

    @Test
    public void testMongoDBStoreConnection() {
        MongoDBStore mongodbStore = MongoDBStore.init("mongodb://hydra.unamurcs.be:27014");
        assertTrue(mongodbStore.listDatabases().size() > 0);
    }
}
