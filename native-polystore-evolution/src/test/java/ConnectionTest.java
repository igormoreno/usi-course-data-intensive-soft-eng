import ch.usi.dataIntensiveSoftEng.polystore.MongoDBStore;
import ch.usi.dataIntensiveSoftEng.polystore.RedisStore;
import ch.usi.dataIntensiveSoftEng.polystore.SqlStore;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class ConnectionTest {
    @Test
    public void testSqlStoreConnection() throws SQLException {
        SqlStore sqlStore = SqlStore.init("jdbc:mysql://hydra.unamurcs.be:33063/reldata", "root", "password");
        assertTrue(sqlStore.getProducts().size() > 0);
    }

    @Test
    public void testRedisStoreConnection() {
        RedisStore redisStore = new RedisStore("hydra.unamurcs.be", 63793);
        assertTrue(redisStore.getStockInfo(52).size() > 0);
    }

    @Test
    public void testMongoDBStoreConnection() {
        MongoDBStore mongodbStore = MongoDBStore.init("mongodb://hydra.unamurcs.be:27013");
        assertTrue(mongodbStore.listDatabases().size() > 0);
    }
}
