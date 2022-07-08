package tdo;

import pojo.Product;
import java.util.List;
import java.util.ArrayList;

public class ProductTDO extends Product {
	private  String reldata_Order_Details_productRef_ProductID; 
	public  String getReldata_Order_Details_productRef_ProductID() {
		return this.reldata_Order_Details_productRef_ProductID;
	}

	public void setReldata_Order_Details_productRef_ProductID(  String reldata_Order_Details_productRef_ProductID) {
		this.reldata_Order_Details_productRef_ProductID = reldata_Order_Details_productRef_ProductID;
	}

	private  String reldata_ProductsInfo_supplierRef_SupplierRef; 
	public  String getReldata_ProductsInfo_supplierRef_SupplierRef() {
		return this.reldata_ProductsInfo_supplierRef_SupplierRef;
	}

	public void setReldata_ProductsInfo_supplierRef_SupplierRef(  String reldata_ProductsInfo_supplierRef_SupplierRef) {
		this.reldata_ProductsInfo_supplierRef_SupplierRef = reldata_ProductsInfo_supplierRef_SupplierRef;
	}

}
