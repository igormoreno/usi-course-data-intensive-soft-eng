package polystore;


import java.util.List;
import java.util.stream.Collectors;

import conditions.Condition;
import conditions.EmployeeAttribute;
import conditions.Operator;
import conditions.OrderAttribute;
import dao.impl.BuysServiceImpl;
import dao.impl.CustomerServiceImpl;
import dao.impl.OrderServiceImpl;
import dao.impl.ProductServiceImpl;
import dao.services.BuysService;
import dao.services.CustomerService;
import dao.services.OrderService;
import dao.services.ProductService;
import pojo.Customer;
import pojo.Order;
import pojo.Product;
import util.Dataset;

public class Polystore {

    /**
     * @return A List of Products containing all information about the products.
     */
    public List<Product> getProducts() {
    	ProductService productService = new ProductServiceImpl();
    	List<Product> products = productService.getProductList();
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
    	OrderService orderService = new OrderServiceImpl();
    	Dataset<Order> orders = orderService.getOrderListInHandlesByEmployeeRefCondition(Condition.simple(EmployeeAttribute.firstName, Operator.EQUALS, employeeFirstName));

    	CustomerService customerService = new CustomerServiceImpl();
		List<Customer> customers =
				orders.stream().map(order -> customerService.getCustomerRefInBuysByBoughtOrder(order)).distinct().collect(Collectors.toList());
        return customers;
    }

    /**
     * Get all detailed Products of Order with given ID.
     *
     * @param orderId Id of related Order.
     * @return All detailed Products of Order with given ID.
     */
    public List<Product> getProductsByOrderId(int orderId) {
    	ProductService productService = new ProductServiceImpl();
		List<Product> products = productService.getProductRefListInComposed_ofByOrderRefCondition(Condition.simple(OrderAttribute.orderID, Operator.EQUALS, orderId));
        return products;
    }
}
