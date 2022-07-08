package ch.usi.dataIntensiveSoftEng.polystore.entities;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EntityUtils {
    public static String prettyPrintProducts(List<Product> products) {
        return format(
                Arrays.asList(
                        "ProductID",
                        "ProductName",
                        "QuantityPerUnit",
                        "UnitPrice",
                        "ReorderLevel",
                        "Discontinued",
                        "UnitsInStock",
                        "UnitsOnOrder"
                ),
                products.stream().map(p -> Arrays.asList(
                        String.valueOf(p.getProductID()),
                        p.getProductName(),
                        p.getQuantityPerUnit(),
                        String.valueOf(p.getUnitPrice()),
                        String.valueOf(p.getReorderLevel()),
                        String.valueOf(p.getDiscontinued()),
                        String.valueOf(p.getUnitsInStock()),
                        String.valueOf(p.getUnitsOnOrder())
                        )
                ).collect(Collectors.toList())
        );
    }

    public static String prettyPrintCustomer(List<Customer> customers) {
        return format(
                Arrays.asList(
                        "Id",
                        "Address",
                        "City",
                        "CompanyName",
                        "ContactName",
                        "ContactTitle",
                        "Country",
                        "Fax",
                        "Phone",
                        "PostalCode",
                        "Region"
                ),
                customers.stream().map(c -> Arrays.asList(
                        c.getId(),
                        c.getAddress(),
                        c.getCity(),
                        c.getCompanyName(),
                        c.getContactName(),
                        c.getContactTitle(),
                        c.getCountry(),
                        c.getFax(),
                        c.getPhone(),
                        c.getPostalCode(),
                        c.getRegion()
                        )
                ).collect(Collectors.toList())
        );
    }

    private static String format(List<String> header, List<List<String>> entries) {
        List<Integer> widths = header.stream().map(s -> s.length()).collect(Collectors.toList());
        for (int col = 0; col < header.size(); col++) {
            int width = widths.get(col);
            for (int row = 0; row < entries.size(); row++) {
                width = Math.max(width, entries.get(row).get(col).length());
                widths.set(col, width);
            }
        }
        return formatLine(widths, header) +
                entries.stream().map(x -> formatLine(widths, x)).collect(Collectors.joining());
    }

    private static String formatLine(List<Integer> widths, List<String> cols) {
        assert widths.size() == cols.size();
        return IntStream.range(0, widths.size())
                        .mapToObj(i -> String.format("%" + widths.get(i) + "s", cols.get(i)))
                        .collect(Collectors.joining("  "))
                        + "\n";
    }
}
