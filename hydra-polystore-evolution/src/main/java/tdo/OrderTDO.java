package tdo;

import pojo.Order;
import java.util.List;
import java.util.ArrayList;

public class OrderTDO extends Order {
	private  String myMongoDB_Orders_orderHandler_source_EmployeeRef; 
	public  String getMyMongoDB_Orders_orderHandler_source_EmployeeRef() {
		return this.myMongoDB_Orders_orderHandler_source_EmployeeRef;
	}

	public void setMyMongoDB_Orders_orderHandler_source_EmployeeRef(  String myMongoDB_Orders_orderHandler_source_EmployeeRef) {
		this.myMongoDB_Orders_orderHandler_source_EmployeeRef = myMongoDB_Orders_orderHandler_source_EmployeeRef;
	}

}
