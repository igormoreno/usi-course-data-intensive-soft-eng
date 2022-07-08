package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

public class RedisStore {
    private final JedisPool pool;

    public RedisStore(String host, int port) {
        this.pool = new JedisPool(host, port);
    }

    public void fillProducts(List<Product> products) {
        Jedis jedis = pool.getResource();
        for (Product p : products) {
            Map<String, String> stockInfo = jedis.hgetAll("PRODUCT:" + p.getProductID() + ":STOCKINFO");
            String unitsInStock = stockInfo.get("UnitsInStock");
            if (unitsInStock != null) {
                p.setUnitsInStock(Integer.parseInt(unitsInStock));
            }
            String unitsOnOrder = stockInfo.get("UnitsOnOrder");
            if (unitsOnOrder != null) {
                p.setUnitsOnOrder(Integer.parseInt(unitsOnOrder));
            }
        }
    }

    public Map<String, String> getStockInfo(int productId) {
        Jedis jedis = pool.getResource();
        return jedis.hgetAll("PRODUCT:" + productId + ":STOCKINFO");
    }
}
