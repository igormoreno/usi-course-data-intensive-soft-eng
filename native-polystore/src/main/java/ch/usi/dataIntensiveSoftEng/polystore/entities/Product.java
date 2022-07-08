package ch.usi.dataIntensiveSoftEng.polystore.entities;

import java.util.Objects;

public class Product {
    private int productID;
    private String productName;
    private String quantityPerUnit;
    private float unitPrice;
    private int reorderLevel;
    private int discontinued;
    private int unitsInStock;
    private int unitsOnOrder;

    public int getProductID() {
        return productID;
    }

    public void setProductID(int productID) {
        this.productID = productID;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getQuantityPerUnit() {
        return quantityPerUnit;
    }

    public void setQuantityPerUnit(String quantityPerUnit) {
        this.quantityPerUnit = quantityPerUnit;
    }

    public float getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(float unitPrice) {
        this.unitPrice = unitPrice;
    }

    public int getReorderLevel() {
        return reorderLevel;
    }

    public void setReorderLevel(int reorderLevel) {
        this.reorderLevel = reorderLevel;
    }

    public int getDiscontinued() {
        return discontinued;
    }

    public void setDiscontinued(int discontinued) {
        this.discontinued = discontinued;
    }

    public int getUnitsInStock() {
        return unitsInStock;
    }

    public void setUnitsInStock(int unitsInStock) {
        this.unitsInStock = unitsInStock;
    }

    public int getUnitsOnOrder() {
        return unitsOnOrder;
    }

    public void setUnitsOnOrder(int unitsOnOrder) {
        this.unitsOnOrder = unitsOnOrder;
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
