package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Customer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MongoDBStore {

    private MongoClient mongoClient;
    private MongoDatabase mongoDB;

    private MongoDBStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.mongoDB = mongoClient.getDatabase("myMongoDB");
    }

    public static MongoDBStore init(String uri) {
        return new MongoDBStore(MongoClients.create(uri));
    }

    public void close() {
        mongoClient.close();
    }

    public List<Customer> getCustomersByOrderEncoder(String employeeFirstName) {
        List<Integer> employeeIds = mongoDB.getCollection("Employees")
                .find(Filters.eq("FirstName", employeeFirstName))
                .map(employee -> employee.getInteger("EmployeeID"))
                .into(new ArrayList<>());
        List<String> customerIds = mongoDB.getCollection("Orders")
                .find(Filters.in("EmployeeRef", employeeIds))
                .map((Document order) -> ((Document)order.get("customer")).getString("CustomerID"))
                .into(new ArrayList<>());
        List<Customer> customers = mongoDB.getCollection("Customers")
                .find(Filters.in("ID", customerIds))
                .map(customer -> new Customer(
                        customer.getString("ID"),
                        customer.getString("Address"),
                        customer.getString("City"),
                        customer.getString("CompanyName"),
                        customer.getString("ContactName"),
                        customer.getString("ContactTitle"),
                        customer.getString("Country"),
                        customer.getString("Fax"),
                        customer.getString("Phone"),
                        customer.getString("PostalCode"),
                        customer.getString("Region")
                ))
                .into(new ArrayList<>());

        return customers;
    }

    public List<Integer> getProductIdsByOrder(int orderId) {
        List<Document> products = (List<Document>) mongoDB.getCollection("Orders")
                .find(Filters.in("OrderID", orderId))
                .first()
                .get("products");
        return products.stream().map(product -> product.getInteger("ProductID")).collect(Collectors.toList());
    }

    public List<Document> listDatabases() {
        return mongoClient.listDatabases().into(new ArrayList<>());
    }
}
