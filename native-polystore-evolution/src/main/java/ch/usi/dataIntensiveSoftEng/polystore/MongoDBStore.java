package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Customer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

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

    public List<Customer> getCustomers() {
        MongoCollection<Document> gradesCollection = mongoDB.getCollection("Customers");
        List<Document> res = gradesCollection.find().into(new ArrayList<>());
        return null;
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

    public List<Document> listDatabases() {
        return mongoClient.listDatabases().into(new ArrayList<>());
    }
}
