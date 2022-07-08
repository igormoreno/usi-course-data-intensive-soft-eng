package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Buys extends LoggingPojo {

	private Order boughtOrder;	
	private Customer customerRef;	

	//Empty constructor
	public Buys() {}
	
	//Role constructor
	public Buys(Order boughtOrder,Customer customerRef){
		this.boughtOrder=boughtOrder;
		this.customerRef=customerRef;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Buys cloned = (Buys) super.clone();
		cloned.setBoughtOrder((Order)cloned.getBoughtOrder().clone());	
		cloned.setCustomerRef((Customer)cloned.getCustomerRef().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "buys : "+
				"roles : {" +  "boughtOrder:Order={"+boughtOrder+"},"+ 
					 "customerRef:Customer={"+customerRef+"}"+
				 "}"+
				"";
	}
	public Order getBoughtOrder() {
		return boughtOrder;
	}	

	public void setBoughtOrder(Order boughtOrder) {
		this.boughtOrder = boughtOrder;
	}
	
	public Customer getCustomerRef() {
		return customerRef;
	}	

	public void setCustomerRef(Customer customerRef) {
		this.customerRef = customerRef;
	}
	

}
