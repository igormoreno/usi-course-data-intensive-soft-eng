package tdo;

import pojo.Product;
import java.util.List;
import java.util.ArrayList;

public class ProductTDO extends Product {
	private  String reldata_Order_Details_productRef_target_ProductID; 
	public  String getReldata_Order_Details_productRef_target_ProductID() {
		return this.reldata_Order_Details_productRef_target_ProductID;
	}

	public void setReldata_Order_Details_productRef_target_ProductID(  String reldata_Order_Details_productRef_target_ProductID) {
		this.reldata_Order_Details_productRef_target_ProductID = reldata_Order_Details_productRef_target_ProductID;
	}

	private  String reldata_ProductsInfo_supplierRef_source_SupplierRef; 
	public  String getReldata_ProductsInfo_supplierRef_source_SupplierRef() {
		return this.reldata_ProductsInfo_supplierRef_source_SupplierRef;
	}

	public void setReldata_ProductsInfo_supplierRef_source_SupplierRef(  String reldata_ProductsInfo_supplierRef_source_SupplierRef) {
		this.reldata_ProductsInfo_supplierRef_source_SupplierRef = reldata_ProductsInfo_supplierRef_source_SupplierRef;
	}

}
