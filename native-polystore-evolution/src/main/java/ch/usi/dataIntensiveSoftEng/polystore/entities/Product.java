package ch.usi.dataIntensiveSoftEng.polystore.entities;

import java.util.Objects;

public class Product {
    private final int productID;
    private final String productName;
    private final String quantityPerUnit;
    private final float unitPrice;
    private final int reorderLevel;
    private final boolean discontinued;
    private final int unitsInStock;
    private final int unitsOnOrder;

    public Product(int productID, String productName, String quantityPerUnit, float unitPrice, int reorderLevel, boolean discontinued, int unitsInStock, int unitsOnOrder) {
        this.productID = productID;
        this.productName = productName;
        this.quantityPerUnit = quantityPerUnit;
        this.unitPrice = unitPrice;
        this.reorderLevel = reorderLevel;
        this.discontinued = discontinued;
        this.unitsInStock = unitsInStock;
        this.unitsOnOrder = unitsOnOrder;
    }

    public int getProductID() {
        return productID;
    }

    public String getProductName() {
        return productName;
    }

    public String getQuantityPerUnit() {
        return quantityPerUnit;
    }

    public float getUnitPrice() {
        return unitPrice;
    }

    public int getReorderLevel() {
        return reorderLevel;
    }

    public boolean getDiscontinued() {
        return discontinued;
    }

    public int getUnitsInStock() {
        return unitsInStock;
    }

    public int getUnitsOnOrder() {
        return unitsOnOrder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Product product = (Product) o;
        return productID == product.productID &&
                Float.compare(product.unitPrice, unitPrice) == 0 &&
                reorderLevel == product.reorderLevel &&
                discontinued == product.discontinued &&
                unitsInStock == product.unitsInStock &&
                unitsOnOrder == product.unitsOnOrder &&
                Objects.equals(productName, product.productName) &&
                Objects.equals(quantityPerUnit, product.quantityPerUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productID, productName, quantityPerUnit, unitPrice, reorderLevel, discontinued, unitsInStock, unitsOnOrder);
    }

    @Override
    public String toString() {
        return "Product{" +
                "productID=" + productID +
                ", productName='" + productName + '\'' +
                ", quantityPerUnit='" + quantityPerUnit + '\'' +
                ", unitPrice=" + unitPrice +
                ", reorderLevel=" + reorderLevel +
                ", discontinued=" + discontinued +
                ", unitsInStock=" + unitsInStock +
                ", unitsOnOrder=" + unitsOnOrder +
                '}';
    }
}
