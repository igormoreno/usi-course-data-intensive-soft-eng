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
        polystore = Polystore.init();
    }

    @After
    public void tearDown() throws Exception {
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
        System.out.println("List of products");
        System.out.println("----------------");
        System.out.println(EntityUtils.format(products));
        System.out.println("----------------");
    }

    /**
     * Test that we can retrieve a List of Customers which have made an Order
     * encoded by Employee with firstname “Margaret”.
     */
    @Ignore
    @Test
    public void getCustomersByOrderEncoder() {
        List<Customer> customers = polystore.getCustomersByOrderEncoder("Margaret");
        assertNotNull(customers);
    }

    /**
     * Test that we can retrieve all detailed Products of Order with ID 10266.
     */
    @Ignore
    @Test
    public void getProductsByOrderId() {
        List<Product> products = polystore.getProductsByOrderId(10266);
        assertNotNull(products);
    }
}