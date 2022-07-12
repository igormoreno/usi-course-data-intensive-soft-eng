package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisStore {
    private final JedisPool pool;
    private final Jedis jedis;

    public RedisStore(String host, int port) {
        this.pool = new JedisPool(host, port);
        this.jedis = pool.getResource();
    }

    public void close() {
        pool.close();
    }

    public List<Product> getProducts() {
        List<Product> products = new ArrayList<>();
        Set<String> keys = jedis.keys("PRODUCT:*");
        for (int i = 1; i <= keys.size(); i++) {
            products.add(getProduct(i));
        }
        return products;
    }

    public Product getProduct(int productId) {
        Map<String, String> productData = jedis.hgetAll("PRODUCT:" + productId);
        return new Product(
                productId,
                productData.get("ProductName"),
                productData.get("QuantityPerUnit"),
                Float.parseFloat(productData.get("UnitPrice")),
                Integer.parseInt(productData.get("ReorderLevel")),
                Boolean.parseBoolean(productData.get("Discontinued")),
                Integer.parseInt(productData.get("UnitsInStock")),
                Integer.parseInt(productData.get("UnitsOnOrder"))
        );
    }
}
