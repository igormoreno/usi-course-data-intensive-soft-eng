package tdo;

import pojo.Supplier;
import java.util.List;
import java.util.ArrayList;

public class SupplierTDO extends Supplier {
	private ArrayList<String> myMongoDB_Suppliers_prodSupplied_source_products = new ArrayList<>(); 
	public ArrayList<String>  getMyMongoDB_Suppliers_prodSupplied_source_products() {
		return this.myMongoDB_Suppliers_prodSupplied_source_products;
	}

	public void setMyMongoDB_Suppliers_prodSupplied_source_products( ArrayList<String>  myMongoDB_Suppliers_prodSupplied_source_products) {
		this.myMongoDB_Suppliers_prodSupplied_source_products = myMongoDB_Suppliers_prodSupplied_source_products;
	}

}
