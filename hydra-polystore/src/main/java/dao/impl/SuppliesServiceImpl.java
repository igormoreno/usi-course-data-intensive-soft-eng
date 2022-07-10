package dao.impl;

import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.commons.lang3.StringUtils;
import util.Dataset;
import conditions.Condition;
import java.util.HashSet;
import java.util.Set;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.SuppliesAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.ProductTDO;
import tdo.SuppliesTDO;
import conditions.ProductAttribute;
import dao.services.ProductService;
import tdo.SupplierTDO;
import tdo.SuppliesTDO;
import conditions.SupplierAttribute;
import dao.services.SupplierService;
import java.util.List;
import java.util.ArrayList;
import util.ScalaUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang3.mutable.MutableBoolean;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Row;
import org.apache.spark.sql.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import static com.mongodb.client.model.Updates.addToSet;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class SuppliesServiceImpl extends dao.services.SuppliesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SuppliesServiceImpl.class);
	
	
	// Left side 'SupplierRef' of reference [supplierRef ]
	public Dataset<ProductTDO> getProductTDOListSuppliedProductInSupplierRefInProductsInfoFromReldata(Condition<ProductAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromReldata(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("reldata", "ProductsInfo", where);
		
	
		Dataset<ProductTDO> res = d.map((MapFunction<Row, ProductTDO>) r -> {
					ProductTDO product_res = new ProductTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Product.ProductID]
					Integer productID = Util.getIntegerValue(r.getAs("ProductID"));
					product_res.setProductID(productID);
					
					// attribute [Product.ProductName]
					String productName = Util.getStringValue(r.getAs("ProductName"));
					product_res.setProductName(productName);
					
					// attribute [Product.QuantityPerUnit]
					String quantityPerUnit = Util.getStringValue(r.getAs("QuantityPerUnit"));
					product_res.setQuantityPerUnit(quantityPerUnit);
					
					// attribute [Product.UnitPrice]
					Double unitPrice = Util.getDoubleValue(r.getAs("UnitPrice"));
					product_res.setUnitPrice(unitPrice);
					
					// attribute [Product.ReorderLevel]
					Integer reorderLevel = Util.getIntegerValue(r.getAs("ReorderLevel"));
					product_res.setReorderLevel(reorderLevel);
					
					// attribute [Product.Discontinued]
					Boolean discontinued = Util.getBooleanValue(r.getAs("Discontinued"));
					product_res.setDiscontinued(discontinued);
	
					// Get reference column [SupplierRef ] for reference [supplierRef]
					String reldata_ProductsInfo_supplierRef_source_SupplierRef = r.getAs("SupplierRef") == null ? null : r.getAs("SupplierRef").toString();
					product_res.setReldata_ProductsInfo_supplierRef_source_SupplierRef(reldata_ProductsInfo_supplierRef_source_SupplierRef);
	
	
					return product_res;
				}, Encoders.bean(ProductTDO.class));
	
	
		return res;
	}
	
	// Right side 'SupplierID' of reference [supplierRef ]
	public Dataset<SupplierTDO> getSupplierTDOListSupplierRefInSupplierRefInProductsInfoFromReldata(Condition<SupplierAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = SupplierServiceImpl.getBSONMatchQueryInSuppliersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Suppliers", bsonQuery);
	
		Dataset<SupplierTDO> res = dataset.flatMap((FlatMapFunction<Row, SupplierTDO>) r -> {
				Set<SupplierTDO> list_res = new HashSet<SupplierTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				SupplierTDO supplier1 = new SupplierTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Supplier.supplierID for field SupplierID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("SupplierID")) {
						if(nestedRow.getAs("SupplierID") == null){
							supplier1.setSupplierID(null);
						}else{
							supplier1.setSupplierID(Util.getIntegerValue(nestedRow.getAs("SupplierID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							supplier1.setAddress(null);
						}else{
							supplier1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							supplier1.setCity(null);
						}else{
							supplier1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.companyName for field CompanyName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CompanyName")) {
						if(nestedRow.getAs("CompanyName") == null){
							supplier1.setCompanyName(null);
						}else{
							supplier1.setCompanyName(Util.getStringValue(nestedRow.getAs("CompanyName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.contactName for field ContactName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName") == null){
							supplier1.setContactName(null);
						}else{
							supplier1.setContactName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.contactTitle for field ContactTitle			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactTitle")) {
						if(nestedRow.getAs("ContactTitle") == null){
							supplier1.setContactTitle(null);
						}else{
							supplier1.setContactTitle(Util.getStringValue(nestedRow.getAs("ContactTitle")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							supplier1.setCountry(null);
						}else{
							supplier1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.fax for field Fax			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Fax")) {
						if(nestedRow.getAs("Fax") == null){
							supplier1.setFax(null);
						}else{
							supplier1.setFax(Util.getStringValue(nestedRow.getAs("Fax")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.homePage for field HomePage			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePage")) {
						if(nestedRow.getAs("HomePage") == null){
							supplier1.setHomePage(null);
						}else{
							supplier1.setHomePage(Util.getStringValue(nestedRow.getAs("HomePage")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.phone for field Phone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Phone")) {
						if(nestedRow.getAs("Phone") == null){
							supplier1.setPhone(null);
						}else{
							supplier1.setPhone(Util.getStringValue(nestedRow.getAs("Phone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							supplier1.setPostalCode(null);
						}else{
							supplier1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Supplier.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							supplier1.setRegion(null);
						}else{
							supplier1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					
						// field  SupplierID for reference supplierRef . Reference field : SupplierID
					nestedRow =  r1;
					if(nestedRow != null) {
						supplier1.setReldata_ProductsInfo_supplierRef_target_SupplierID(nestedRow.getAs("SupplierID") == null ? null : nestedRow.getAs("SupplierID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(supplier1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(SupplierTDO.class));
		res= res.dropDuplicates(new String[]{"supplierID"});
		return res;
	}
	
	
	
	
	public Dataset<Supplies> getSuppliesList(
		Condition<ProductAttribute> suppliedProduct_condition,
		Condition<SupplierAttribute> supplierRef_condition){
			SuppliesServiceImpl suppliesService = this;
			ProductService productService = new ProductServiceImpl();  
			SupplierService supplierService = new SupplierServiceImpl();
			MutableBoolean suppliedProduct_refilter = new MutableBoolean(false);
			List<Dataset<Supplies>> datasetsPOJO = new ArrayList<Dataset<Supplies>>();
			boolean all_already_persisted = false;
			MutableBoolean supplierRef_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'suppliedProduct' in reference 'supplierRef'. A->B Scenario
			supplierRef_refilter = new MutableBoolean(false);
			Dataset<ProductTDO> productTDOsupplierRefsuppliedProduct = suppliesService.getProductTDOListSuppliedProductInSupplierRefInProductsInfoFromReldata(suppliedProduct_condition, suppliedProduct_refilter);
			Dataset<SupplierTDO> supplierTDOsupplierRefsupplierRef = suppliesService.getSupplierTDOListSupplierRefInSupplierRefInProductsInfoFromReldata(supplierRef_condition, supplierRef_refilter);
			
			Dataset<Row> res_supplierRef_temp = productTDOsupplierRefsuppliedProduct.join(supplierTDOsupplierRefsupplierRef
					.withColumnRenamed("supplierID", "Supplier_supplierID")
					.withColumnRenamed("address", "Supplier_address")
					.withColumnRenamed("city", "Supplier_city")
					.withColumnRenamed("companyName", "Supplier_companyName")
					.withColumnRenamed("contactName", "Supplier_contactName")
					.withColumnRenamed("contactTitle", "Supplier_contactTitle")
					.withColumnRenamed("country", "Supplier_country")
					.withColumnRenamed("fax", "Supplier_fax")
					.withColumnRenamed("homePage", "Supplier_homePage")
					.withColumnRenamed("phone", "Supplier_phone")
					.withColumnRenamed("postalCode", "Supplier_postalCode")
					.withColumnRenamed("region", "Supplier_region")
					.withColumnRenamed("logEvents", "Supplier_logEvents"),
					productTDOsupplierRefsuppliedProduct.col("reldata_ProductsInfo_supplierRef_source_SupplierRef").equalTo(supplierTDOsupplierRefsupplierRef.col("reldata_ProductsInfo_supplierRef_target_SupplierID")));
		
			Dataset<Supplies> res_supplierRef = res_supplierRef_temp.map(
				(MapFunction<Row, Supplies>) r -> {
					Supplies res = new Supplies();
					Product A = new Product();
					Supplier B = new Supplier();
					A.setProductID(Util.getIntegerValue(r.getAs("productID")));
					A.setUnitsInStock(Util.getIntegerValue(r.getAs("unitsInStock")));
					A.setUnitsOnOrder(Util.getIntegerValue(r.getAs("unitsOnOrder")));
					A.setProductName(Util.getStringValue(r.getAs("productName")));
					A.setQuantityPerUnit(Util.getStringValue(r.getAs("quantityPerUnit")));
					A.setUnitPrice(Util.getDoubleValue(r.getAs("unitPrice")));
					A.setReorderLevel(Util.getIntegerValue(r.getAs("reorderLevel")));
					A.setDiscontinued(Util.getBooleanValue(r.getAs("discontinued")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setSupplierID(Util.getIntegerValue(r.getAs("Supplier_supplierID")));
					B.setAddress(Util.getStringValue(r.getAs("Supplier_address")));
					B.setCity(Util.getStringValue(r.getAs("Supplier_city")));
					B.setCompanyName(Util.getStringValue(r.getAs("Supplier_companyName")));
					B.setContactName(Util.getStringValue(r.getAs("Supplier_contactName")));
					B.setContactTitle(Util.getStringValue(r.getAs("Supplier_contactTitle")));
					B.setCountry(Util.getStringValue(r.getAs("Supplier_country")));
					B.setFax(Util.getStringValue(r.getAs("Supplier_fax")));
					B.setHomePage(Util.getStringValue(r.getAs("Supplier_homePage")));
					B.setPhone(Util.getStringValue(r.getAs("Supplier_phone")));
					B.setPostalCode(Util.getStringValue(r.getAs("Supplier_postalCode")));
					B.setRegion(Util.getStringValue(r.getAs("Supplier_region")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Supplier_logEvents")));
						
					res.setSuppliedProduct(A);
					res.setSupplierRef(B);
					return res;
				},Encoders.bean(Supplies.class)
			);
		
			datasetsPOJO.add(res_supplierRef);
		
			
			Dataset<Supplies> res_supplies_suppliedProduct;
			Dataset<Product> res_Product;
			
			
			//Join datasets or return 
			Dataset<Supplies> res = fullOuterJoinsSupplies(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Product> lonelySuppliedProduct = null;
			Dataset<Supplier> lonelySupplierRef = null;
			
			List<Dataset<Product>> lonelysuppliedProductList = new ArrayList<Dataset<Product>>();
			lonelysuppliedProductList.add(productService.getProductListInProductStockInfoFromMyRedis(suppliedProduct_condition, new MutableBoolean(false)));
			lonelySuppliedProduct = ProductService.fullOuterJoinsProduct(lonelysuppliedProductList);
			if(lonelySuppliedProduct != null) {
				res = fullLeftOuterJoinBetweenSuppliesAndSuppliedProduct(res, lonelySuppliedProduct);
			}	
		
		
			
			if(suppliedProduct_refilter.booleanValue() || supplierRef_refilter.booleanValue())
				res = res.filter((FilterFunction<Supplies>) r -> (suppliedProduct_condition == null || suppliedProduct_condition.evaluate(r.getSuppliedProduct())) && (supplierRef_condition == null || supplierRef_condition.evaluate(r.getSupplierRef())));
			
		
			return res;
		
		}
	
	public Dataset<Supplies> getSuppliesListBySuppliedProductCondition(
		Condition<ProductAttribute> suppliedProduct_condition
	){
		return getSuppliesList(suppliedProduct_condition, null);
	}
	
	public Supplies getSuppliesBySuppliedProduct(Product suppliedProduct) {
		Condition<ProductAttribute> cond = null;
		cond = Condition.simple(ProductAttribute.productID, Operator.EQUALS, suppliedProduct.getProductID());
		Dataset<Supplies> res = getSuppliesListBySuppliedProductCondition(cond);
		List<Supplies> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Supplies> getSuppliesListBySupplierRefCondition(
		Condition<SupplierAttribute> supplierRef_condition
	){
		return getSuppliesList(null, supplierRef_condition);
	}
	
	public Dataset<Supplies> getSuppliesListBySupplierRef(Supplier supplierRef) {
		Condition<SupplierAttribute> cond = null;
		cond = Condition.simple(SupplierAttribute.supplierID, Operator.EQUALS, supplierRef.getSupplierID());
		Dataset<Supplies> res = getSuppliesListBySupplierRefCondition(cond);
	return res;
	}
	
	
	
	public void deleteSuppliesList(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition){
			//TODO
		}
	
	public void deleteSuppliesListBySuppliedProductCondition(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition
	){
		deleteSuppliesList(suppliedProduct_condition, null);
	}
	
	public void deleteSuppliesBySuppliedProduct(pojo.Product suppliedProduct) {
		// TODO using id for selecting
		return;
	}
	public void deleteSuppliesListBySupplierRefCondition(
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition
	){
		deleteSuppliesList(null, supplierRef_condition);
	}
	
	public void deleteSuppliesListBySupplierRef(pojo.Supplier supplierRef) {
		// TODO using id for selecting
		return;
	}
		
}
