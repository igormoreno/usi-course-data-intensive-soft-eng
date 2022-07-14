package tdo;

import pojo.Product;
import java.util.List;
import java.util.ArrayList;

public class ProductTDO extends Product {
	private  String myMongoDB_Suppliers_prodSupplied_target_ProductID;
	public  String getMyMongoDB_Suppliers_prodSupplied_target_ProductID() {
		return this.myMongoDB_Suppliers_prodSupplied_target_ProductID;
	}

	public void setMyMongoDB_Suppliers_prodSupplied_target_ProductID(  String myMongoDB_Suppliers_prodSupplied_target_ProductID) {
		this.myMongoDB_Suppliers_prodSupplied_target_ProductID = myMongoDB_Suppliers_prodSupplied_target_ProductID;
	}

}
