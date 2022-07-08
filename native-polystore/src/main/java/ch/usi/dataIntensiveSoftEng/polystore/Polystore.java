package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Customer;
import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;

import java.sql.SQLException;
import java.util.List;

public class Polystore {
    private SqlStore sqlStore;
    private RedisStore redisStore;
    private MongoDBStore mongoDBStore;

    private Polystore(SqlStore sqlStore, RedisStore redisStore, MongoDBStore mongoDBStore) {
        this.sqlStore = sqlStore;
        this.redisStore = redisStore;
        this.mongoDBStore = mongoDBStore;
    }

    public static Polystore init() throws SQLException {
       return new Polystore(
               SqlStore.init("jdbc:mysql://hydra.unamurcs.be:33063/reldata", "root", "password"),
               new RedisStore("hydra.unamurcs.be", 63793),
               MongoDBStore.init("mongodb://hydra.unamurcs.be:27013")
       );
    }

    /**
     * @return A List of Products containing all information about the products.
     */
    public List<Product> getProducts() throws SQLException {
        List<Product> products = sqlStore.getProducts();
        redisStore.fillProducts(products);
        return products;
    }

    /**
     * Returns a List of Customers which have made an Order encoded by
     * Employee with given firstname.
     *
     * @param employeeFirstName Employee first name.
     * @return A List of Customers which have made an Order encoded by
     *         Employee with given firstname.
     */
    public List<Customer> getCustomersByOrderEncoder(String employeeFirstName) {
        return null;
    }

    /**
     * Get all detailed Products of Order with given ID.
     *
     * @param orderId Id of related Order.
     * @return All detailed Products of Order with given ID.
     */
    public List<Product> getProductsByOrderId(int orderId) {
        return null;
    }
}
