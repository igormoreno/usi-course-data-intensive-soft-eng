import ch.usi.dataIntensiveSoftEng.polystore.MongoDBStore;
import ch.usi.dataIntensiveSoftEng.polystore.RedisStore;
import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;
import org.bson.Document;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ConnectionTest {
    @Test
    public void testRedisStoreConnection() {
        RedisStore redisStore = new RedisStore("hydra.unamurcs.be", 63794);
        Product p = redisStore.getProduct(52);
        assertNotNull(p);
    }

    @Test
    public void testMongoDBStoreConnection() {
        MongoDBStore mongodbStore = MongoDBStore.init("mongodb://hydra.unamurcs.be:27014");
        List<Document> dbs = mongodbStore.listDatabases();
        assertTrue(dbs.size() > 0);
    }
}
