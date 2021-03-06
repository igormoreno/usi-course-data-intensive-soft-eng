package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Customer;
import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class Polystore {
    private RedisStore redisStore;
    private MongoDBStore mongoDBStore;

    public Polystore(RedisStore redisStore, MongoDBStore mongoDBStore) {
        this.redisStore = redisStore;
        this.mongoDBStore = mongoDBStore;
    }

    public void close() {
        mongoDBStore.close();
        redisStore.close();
    }

    /**
     * @return A List of Products containing all information about the products.
     */
    public List<Product> getProducts() throws SQLException {
        return redisStore.getProducts();
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
        return mongoDBStore.getCustomersByOrderEncoder(employeeFirstName);
    }

    /**
     * Get all detailed Products of Order with given ID.
     *
     * @param orderId Id of related Order.
     * @return All detailed Products of Order with given ID.
     */
    public List<Product> getProductsByOrderId(int orderId) throws SQLException {
        return mongoDBStore.getProductIdsByOrder(orderId)
                           .stream()
                           .map(id -> redisStore.getProduct(id))
                           .collect(Collectors.toList());
    }
}
