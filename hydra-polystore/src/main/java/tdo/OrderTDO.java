package tdo;

import pojo.Order;
import java.util.List;
import java.util.ArrayList;

public class OrderTDO extends Order {
	private  String reldata_Order_Details_orderRef_OrderID; 
	public  String getReldata_Order_Details_orderRef_OrderID() {
		return this.reldata_Order_Details_orderRef_OrderID;
	}

	public void setReldata_Order_Details_orderRef_OrderID(  String reldata_Order_Details_orderRef_OrderID) {
		this.reldata_Order_Details_orderRef_OrderID = reldata_Order_Details_orderRef_OrderID;
	}

	private  String myMongoDB_Orders_orderHandler_EmployeeRef; 
	public  String getMyMongoDB_Orders_orderHandler_EmployeeRef() {
		return this.myMongoDB_Orders_orderHandler_EmployeeRef;
	}

	public void setMyMongoDB_Orders_orderHandler_EmployeeRef(  String myMongoDB_Orders_orderHandler_EmployeeRef) {
		this.myMongoDB_Orders_orderHandler_EmployeeRef = myMongoDB_Orders_orderHandler_EmployeeRef;
	}

}
