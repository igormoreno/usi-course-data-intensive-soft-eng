package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Handles extends LoggingPojo {

	private Order order;	
	private Employee employeeRef;	

	//Empty constructor
	public Handles() {}
	
	//Role constructor
	public Handles(Order order,Employee employeeRef){
		this.order=order;
		this.employeeRef=employeeRef;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Handles cloned = (Handles) super.clone();
		cloned.setOrder((Order)cloned.getOrder().clone());	
		cloned.setEmployeeRef((Employee)cloned.getEmployeeRef().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "handles : "+
				"roles : {" +  "order:Order={"+order+"},"+ 
					 "employeeRef:Employee={"+employeeRef+"}"+
				 "}"+
				"";
	}
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	
	public Employee getEmployeeRef() {
		return employeeRef;
	}	

	public void setEmployeeRef(Employee employeeRef) {
		this.employeeRef = employeeRef;
	}
	

}
