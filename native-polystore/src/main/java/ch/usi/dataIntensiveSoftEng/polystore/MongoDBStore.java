package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Customer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

    public List<Document> listDatabases() {
        return mongoClient.listDatabases().into(new ArrayList<>());
    }
}
