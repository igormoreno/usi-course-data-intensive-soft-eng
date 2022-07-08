package ch.usi.dataIntensiveSoftEng.polystore.entities;

import java.util.List;

public class EntityUtils {
    public static String format(List<Product> products) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "%-9s %-40s %-30s %9s %-12s %-12s %-12s %-12s\n",
                "ProductID",
                "ProductName",
                "QuantityPerUnit",
                "UnitPrice",
                "ReorderLevel",
                "Discontinued",
                "UnitsInStock",
                "UnitsOnOrder"
        ));
        for (Product p : products) {
            sb.append(String.format(
                    "%9d %-40s %-30s %9.2f %12d %12d %12d %12d\n",
                    p.getProductID(),
                    p.getProductName(),
                    p.getQuantityPerUnit(),
                    p.getUnitPrice(),
                    p.getReorderLevel(),
                    p.getDiscontinued(),
                    p.getUnitsInStock(),
                    p.getUnitsOnOrder()
            ));
        }
        return sb.toString();
    }
}
