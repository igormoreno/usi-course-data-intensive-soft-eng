package polystore;


import java.util.List;
import conditions.Condition;
import conditions.Operator;
import conditions.OrderAttribute;
import dao.impl.ProductServiceImpl;
import dao.services.ProductService;
import pojo.Customer;
import pojo.Product;

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
//    	OrderService orderService = new OrderServiceImpl();
//    	List<Integer> orders = orderService.getOrderListInHandlesByEmployeeRefCondition(Condition.simple(EmployeeAttribute.firstName, Operator.EQUALS, employeeFirstName))
//    			                         .stream().map(order -> order.getOrderID()).collect(Collectors.toList());
//
//    	CustomerService customerService = new CustomerServiceImpl();
//		Dataset<Customer> customers = customerService.getCustomerRefListInBuysByBoughtOrderCondition(Condition.createOrCondition(OrderAttribute.orderID, Operator.EQUALS, orders.toArray()));
//        return customers;
    	return null;
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
