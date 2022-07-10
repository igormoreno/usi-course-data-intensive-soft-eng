package tdo;

import pojo.Order;
import java.util.List;
import java.util.ArrayList;

public class OrderTDO extends Order {
	private  String reldata_Order_Details_orderRef_target_OrderID; 
	public  String getReldata_Order_Details_orderRef_target_OrderID() {
		return this.reldata_Order_Details_orderRef_target_OrderID;
	}

	public void setReldata_Order_Details_orderRef_target_OrderID(  String reldata_Order_Details_orderRef_target_OrderID) {
		this.reldata_Order_Details_orderRef_target_OrderID = reldata_Order_Details_orderRef_target_OrderID;
	}

	private  String myMongoDB_Orders_orderHandler_source_EmployeeRef; 
	public  String getMyMongoDB_Orders_orderHandler_source_EmployeeRef() {
		return this.myMongoDB_Orders_orderHandler_source_EmployeeRef;
	}

	public void setMyMongoDB_Orders_orderHandler_source_EmployeeRef(  String myMongoDB_Orders_orderHandler_source_EmployeeRef) {
		this.myMongoDB_Orders_orderHandler_source_EmployeeRef = myMongoDB_Orders_orderHandler_source_EmployeeRef;
	}

}
