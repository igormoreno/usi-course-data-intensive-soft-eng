package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Customer;
import ch.usi.dataIntensiveSoftEng.polystore.entities.EntityUtils;
import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class PolystoreTest {

    private Polystore polystore;

    @Before
    public void setUp() throws Exception {
        RedisStore redisStore = new RedisStore("hydra.unamurcs.be", 63794);
        MongoDBStore mongoDBStore = MongoDBStore.init("mongodb://hydra.unamurcs.be:27014");
        polystore = new Polystore(redisStore, mongoDBStore);
    }

    @After
    public void tearDown() throws Exception {
        polystore.close();
    }

    /**
     * Test that we can retrieve a List of Products containing all information
     * about the products.
     */
    @Test
    public void getProducts() throws SQLException {
        List<Product> products = polystore.getProducts();
        assertNotNull(products);
        assertEquals(77, products.size());
        System.out.println("\nList of Products containing all information about the products (" + products.size() + ")");
        System.out.println("----------------");
        System.out.println(EntityUtils.prettyPrintProducts(products));
        System.out.println("----------------");
    }

    /**
     * Test that we can retrieve a List of Customers which have made an Order
     * encoded by Employee with firstname “Margaret”.
     */
    @Test
    public void getCustomersByOrderEncoder() {
        List<Customer> customers = polystore.getCustomersByOrderEncoder("Margaret");
        assertNotNull(customers);
        assertEquals(75, customers.size());
        System.out.println("\nList of Customers which have made an Order encoded by Employee with firstname \"Margaret\" (" + customers.size() + ")");
        System.out.println("----------------");
        System.out.println(EntityUtils.prettyPrintCustomer(customers));
        System.out.println("----------------");
    }

    /**
     * Test that we can retrieve all detailed Products of Order with ID 10266.
     */
    @Test
    public void getProductsByOrderId() throws SQLException {
        List<Product> products = polystore.getProductsByOrderId(10266);
        assertNotNull(products);
        assertEquals(1, products.size());
        assertEquals(12, products.get(0).getProductID());
        System.out.println("\nAll detailed Products of Order with ID 10266 (" + products.size() + ")");
        System.out.println("----------------");
        System.out.println(EntityUtils.prettyPrintProducts(products));
        System.out.println("----------------");
    }
}