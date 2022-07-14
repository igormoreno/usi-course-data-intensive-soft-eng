package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Product extends LoggingPojo {

	private Integer productID;
	private Integer unitsInStock;
	private Integer unitsOnOrder;
	private String productName;
	private String quantityPerUnit;
	private Double unitPrice;
	private Integer reorderLevel;
	private Boolean discontinued;

	private List<Composed_of> composed_ofListAsProductRef;
	public enum supplies {
		suppliedProduct
	}
	private Supplier supplierRef;

	// Empty constructor
	public Product() {}

	// Constructor on Identifier
	public Product(Integer productID){
		this.productID = productID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Product(Integer productID,Integer unitsInStock,Integer unitsOnOrder,String productName,String quantityPerUnit,Double unitPrice,Integer reorderLevel,Boolean discontinued) {
		this.productID = productID;
		this.unitsInStock = unitsInStock;
		this.unitsOnOrder = unitsOnOrder;
		this.productName = productName;
		this.quantityPerUnit = quantityPerUnit;
		this.unitPrice = unitPrice;
		this.reorderLevel = reorderLevel;
		this.discontinued = discontinued;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Product Product = (Product) o;
		boolean eqSimpleAttr = Objects.equals(productID,Product.productID) && Objects.equals(unitsInStock,Product.unitsInStock) && Objects.equals(unitsOnOrder,Product.unitsOnOrder) && Objects.equals(productName,Product.productName) && Objects.equals(quantityPerUnit,Product.quantityPerUnit) && Objects.equals(unitPrice,Product.unitPrice) && Objects.equals(reorderLevel,Product.reorderLevel) && Objects.equals(discontinued,Product.discontinued);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(composed_ofListAsProductRef,Product.composed_ofListAsProductRef) &&
	Objects.equals(supplierRef, Product.supplierRef) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Product { " + "productID="+productID +", "+
					"unitsInStock="+unitsInStock +", "+
					"unitsOnOrder="+unitsOnOrder +", "+
					"productName="+productName +", "+
					"quantityPerUnit="+quantityPerUnit +", "+
					"unitPrice="+unitPrice +", "+
					"reorderLevel="+reorderLevel +", "+
					"discontinued="+discontinued +"}"; 
	}
	
	public Integer getProductID() {
		return productID;
	}

	public void setProductID(Integer productID) {
		this.productID = productID;
	}
	public Integer getUnitsInStock() {
		return unitsInStock;
	}

	public void setUnitsInStock(Integer unitsInStock) {
		this.unitsInStock = unitsInStock;
	}
	public Integer getUnitsOnOrder() {
		return unitsOnOrder;
	}

	public void setUnitsOnOrder(Integer unitsOnOrder) {
		this.unitsOnOrder = unitsOnOrder;
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
	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}
	public Integer getReorderLevel() {
		return reorderLevel;
	}

	public void setReorderLevel(Integer reorderLevel) {
		this.reorderLevel = reorderLevel;
	}
	public Boolean getDiscontinued() {
		return discontinued;
	}

	public void setDiscontinued(Boolean discontinued) {
		this.discontinued = discontinued;
	}

	

	public java.util.List<Composed_of> _getComposed_ofListAsProductRef() {
		return composed_ofListAsProductRef;
	}

	public void _setComposed_ofListAsProductRef(java.util.List<Composed_of> composed_ofListAsProductRef) {
		this.composed_ofListAsProductRef = composed_ofListAsProductRef;
	}
	public Supplier _getSupplierRef() {
		return supplierRef;
	}

	public void _setSupplierRef(Supplier supplierRef) {
		this.supplierRef = supplierRef;
	}
}
