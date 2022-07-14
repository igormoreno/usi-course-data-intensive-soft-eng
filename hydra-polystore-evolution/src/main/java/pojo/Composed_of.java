package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Composed_of extends LoggingPojo {

	private Order orderRef;	
	private Product productRef;	
	private Double unitPrice;	
	private Integer quantity;	
	private Double discount;	

	//Empty constructor
	public Composed_of() {}
	
	//Role constructor
	public Composed_of(Order orderRef,Product productRef){
		this.orderRef=orderRef;
		this.productRef=productRef;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Composed_of cloned = (Composed_of) super.clone();
		cloned.setOrderRef((Order)cloned.getOrderRef().clone());	
		cloned.setProductRef((Product)cloned.getProductRef().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "composed_of : "+
				"roles : {" +  "orderRef:Order={"+orderRef+"},"+ 
					 "productRef:Product={"+productRef+"}"+
				 "}"+
			" attributes : { " + "unitPrice="+unitPrice +", "+
					"quantity="+quantity +", "+
					"discount="+discount +"}"; 
	}
	public Order getOrderRef() {
		return orderRef;
	}	

	public void setOrderRef(Order orderRef) {
		this.orderRef = orderRef;
	}
	
	public Product getProductRef() {
		return productRef;
	}	

	public void setProductRef(Product productRef) {
		this.productRef = productRef;
	}
	
	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}
	
	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	
	public Double getDiscount() {
		return discount;
	}

	public void setDiscount(Double discount) {
		this.discount = discount;
	}
	

}
