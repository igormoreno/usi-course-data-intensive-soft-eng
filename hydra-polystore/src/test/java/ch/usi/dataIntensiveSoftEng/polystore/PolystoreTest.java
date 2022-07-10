package ch.usi.dataIntensiveSoftEng.polystore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import pojo.Customer;
import pojo.Product;
import polystore.Polystore;

public class PolystoreTest {

    private Polystore polystore = new Polystore();

    @BeforeAll
    public void setUp() throws Exception {
    }

    @AfterAll
    public void tearDown() throws Exception {
    }

    /**
     * Test that we can retrieve a List of Products containing all information
     * about the products.
     */
    @Test
    public void testGetProducts() throws SQLException {
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
    public void testGetCustomersByOrderEncoder() {
        List<Customer> customers = polystore.getCustomersByOrderEncoder("Margaret");
//        assertNotNull(customers);
//        System.out.println("\nList of Customers which have made an Order encoded by Employee with firstname \"Margaret\" (" + customers.size() + ")");
//        System.out.println("----------------");
//        System.out.println(EntityUtils.prettyPrintCustomer(customers));
//        System.out.println("----------------");
    }

    /**
     * Test that we can retrieve all detailed Products of Order with ID 10266.
     */
    @Test
    public void testGetProductsByOrderId() throws SQLException {
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