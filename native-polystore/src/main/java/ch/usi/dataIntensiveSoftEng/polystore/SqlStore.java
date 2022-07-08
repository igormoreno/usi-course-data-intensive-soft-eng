package ch.usi.dataIntensiveSoftEng.polystore;

import ch.usi.dataIntensiveSoftEng.polystore.entities.Product;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqlStore {
    private final Connection connection;

    private SqlStore(Connection connection) {
        this.connection = connection;
    }

    public static SqlStore init(String url, String user, String password) throws SQLException {
        //Class.forName("com.mysql.cj.jdbc.Driver");
        return new SqlStore(DriverManager.getConnection(url, user, password));
    }

    public List<Product> getProducts() throws SQLException {
        PreparedStatement ps = connection.prepareStatement("SELECT * FROM ProductsInfo");
        ResultSet rs = ps.executeQuery();

        List<Product> res = new ArrayList<>();
        while (rs.next()) {
            Product p = new Product();
            p.setProductID(rs.getInt("ProductID"));
            p.setProductName(rs.getString("ProductName"));
            p.setQuantityPerUnit(rs.getString("QuantityPerUnit"));
            p.setUnitPrice(rs.getFloat("UnitPrice"));
            p.setReorderLevel(rs.getInt("ReorderLevel"));
            p.setDiscontinued(rs.getInt("Discontinued"));
            res.add(p);
        }
        return res;
    }
}
