package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Supplies extends LoggingPojo {

	private Product suppliedProduct;	
	private Supplier supplierRef;	

	//Empty constructor
	public Supplies() {}
	
	//Role constructor
	public Supplies(Product suppliedProduct,Supplier supplierRef){
		this.suppliedProduct=suppliedProduct;
		this.supplierRef=supplierRef;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Supplies cloned = (Supplies) super.clone();
		cloned.setSuppliedProduct((Product)cloned.getSuppliedProduct().clone());	
		cloned.setSupplierRef((Supplier)cloned.getSupplierRef().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "supplies : "+
				"roles : {" +  "suppliedProduct:Product={"+suppliedProduct+"},"+ 
					 "supplierRef:Supplier={"+supplierRef+"}"+
				 "}"+
				"";
	}
	public Product getSuppliedProduct() {
		return suppliedProduct;
	}	

	public void setSuppliedProduct(Product suppliedProduct) {
		this.suppliedProduct = suppliedProduct;
	}
	
	public Supplier getSupplierRef() {
		return supplierRef;
	}	

	public void setSupplierRef(Supplier supplierRef) {
		this.supplierRef = supplierRef;
	}
	

}
