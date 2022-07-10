package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Supplies;
import java.time.LocalDate;
import java.time.LocalDateTime;
import tdo.*;
import pojo.*;
import org.apache.commons.lang3.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.*;


public abstract class SuppliesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SuppliesService.class);
	
	
	// Left side 'SupplierRef' of reference [supplierRef ]
	public abstract Dataset<ProductTDO> getProductTDOListSuppliedProductInSupplierRefInProductsInfoFromReldata(Condition<ProductAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'SupplierID' of reference [supplierRef ]
	public abstract Dataset<SupplierTDO> getSupplierTDOListSupplierRefInSupplierRefInProductsInfoFromReldata(Condition<SupplierAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Supplies> fullLeftOuterJoinBetweenSuppliesAndSuppliedProduct(Dataset<Supplies> d1, Dataset<Product> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("productID", "A_productID")
			.withColumnRenamed("unitsInStock", "A_unitsInStock")
			.withColumnRenamed("unitsOnOrder", "A_unitsOnOrder")
			.withColumnRenamed("productName", "A_productName")
			.withColumnRenamed("quantityPerUnit", "A_quantityPerUnit")
			.withColumnRenamed("unitPrice", "A_unitPrice")
			.withColumnRenamed("reorderLevel", "A_reorderLevel")
			.withColumnRenamed("discontinued", "A_discontinued")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("suppliedProduct.productID").equalTo(d2_.col("A_productID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Supplies>) r -> {
				Supplies res = new Supplies();
	
				Product suppliedProduct = new Product();
				Object o = r.getAs("suppliedProduct");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						suppliedProduct.setProductID(Util.getIntegerValue(r2.getAs("productID")));
						suppliedProduct.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						suppliedProduct.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						suppliedProduct.setProductName(Util.getStringValue(r2.getAs("productName")));
						suppliedProduct.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						suppliedProduct.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						suppliedProduct.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						suppliedProduct.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Product) {
						suppliedProduct = (Product) o;
					}
				}
	
				res.setSuppliedProduct(suppliedProduct);
	
				Integer productID = Util.getIntegerValue(r.getAs("A_productID"));
				if (suppliedProduct.getProductID() != null && productID != null && !suppliedProduct.getProductID().equals(productID)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.productID': " + suppliedProduct.getProductID() + " and " + productID + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.productID': " + suppliedProduct.getProductID() + " and " + productID + "." );
				}
				if(productID != null)
					suppliedProduct.setProductID(productID);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (suppliedProduct.getUnitsInStock() != null && unitsInStock != null && !suppliedProduct.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.unitsInStock': " + suppliedProduct.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.unitsInStock': " + suppliedProduct.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					suppliedProduct.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (suppliedProduct.getUnitsOnOrder() != null && unitsOnOrder != null && !suppliedProduct.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.unitsOnOrder': " + suppliedProduct.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.unitsOnOrder': " + suppliedProduct.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					suppliedProduct.setUnitsOnOrder(unitsOnOrder);
				String productName = Util.getStringValue(r.getAs("A_productName"));
				if (suppliedProduct.getProductName() != null && productName != null && !suppliedProduct.getProductName().equals(productName)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.productName': " + suppliedProduct.getProductName() + " and " + productName + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.productName': " + suppliedProduct.getProductName() + " and " + productName + "." );
				}
				if(productName != null)
					suppliedProduct.setProductName(productName);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (suppliedProduct.getQuantityPerUnit() != null && quantityPerUnit != null && !suppliedProduct.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.quantityPerUnit': " + suppliedProduct.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.quantityPerUnit': " + suppliedProduct.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					suppliedProduct.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (suppliedProduct.getUnitPrice() != null && unitPrice != null && !suppliedProduct.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.unitPrice': " + suppliedProduct.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.unitPrice': " + suppliedProduct.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					suppliedProduct.setUnitPrice(unitPrice);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (suppliedProduct.getReorderLevel() != null && reorderLevel != null && !suppliedProduct.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.reorderLevel': " + suppliedProduct.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.reorderLevel': " + suppliedProduct.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					suppliedProduct.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (suppliedProduct.getDiscontinued() != null && discontinued != null && !suppliedProduct.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.discontinued': " + suppliedProduct.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.discontinued': " + suppliedProduct.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					suppliedProduct.setDiscontinued(discontinued);
	
				o = r.getAs("supplierRef");
				Supplier supplierRef = new Supplier();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						supplierRef.setSupplierID(Util.getIntegerValue(r2.getAs("supplierID")));
						supplierRef.setAddress(Util.getStringValue(r2.getAs("address")));
						supplierRef.setCity(Util.getStringValue(r2.getAs("city")));
						supplierRef.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						supplierRef.setContactName(Util.getStringValue(r2.getAs("contactName")));
						supplierRef.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						supplierRef.setCountry(Util.getStringValue(r2.getAs("country")));
						supplierRef.setFax(Util.getStringValue(r2.getAs("fax")));
						supplierRef.setHomePage(Util.getStringValue(r2.getAs("homePage")));
						supplierRef.setPhone(Util.getStringValue(r2.getAs("phone")));
						supplierRef.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						supplierRef.setRegion(Util.getStringValue(r2.getAs("region")));
					} 
					if(o instanceof Supplier) {
						supplierRef = (Supplier) o;
					}
				}
	
				res.setSupplierRef(supplierRef);
	
				return res;
		}, Encoders.bean(Supplies.class));
	
		
		
	}
	public static Dataset<Supplies> fullLeftOuterJoinBetweenSuppliesAndSupplierRef(Dataset<Supplies> d1, Dataset<Supplier> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("supplierID", "A_supplierID")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("contactName", "A_contactName")
			.withColumnRenamed("contactTitle", "A_contactTitle")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("fax", "A_fax")
			.withColumnRenamed("homePage", "A_homePage")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("supplierRef.supplierID").equalTo(d2_.col("A_supplierID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Supplies>) r -> {
				Supplies res = new Supplies();
	
				Supplier supplierRef = new Supplier();
				Object o = r.getAs("supplierRef");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						supplierRef.setSupplierID(Util.getIntegerValue(r2.getAs("supplierID")));
						supplierRef.setAddress(Util.getStringValue(r2.getAs("address")));
						supplierRef.setCity(Util.getStringValue(r2.getAs("city")));
						supplierRef.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						supplierRef.setContactName(Util.getStringValue(r2.getAs("contactName")));
						supplierRef.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						supplierRef.setCountry(Util.getStringValue(r2.getAs("country")));
						supplierRef.setFax(Util.getStringValue(r2.getAs("fax")));
						supplierRef.setHomePage(Util.getStringValue(r2.getAs("homePage")));
						supplierRef.setPhone(Util.getStringValue(r2.getAs("phone")));
						supplierRef.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						supplierRef.setRegion(Util.getStringValue(r2.getAs("region")));
					} 
					if(o instanceof Supplier) {
						supplierRef = (Supplier) o;
					}
				}
	
				res.setSupplierRef(supplierRef);
	
				Integer supplierID = Util.getIntegerValue(r.getAs("A_supplierID"));
				if (supplierRef.getSupplierID() != null && supplierID != null && !supplierRef.getSupplierID().equals(supplierID)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.supplierID': " + supplierRef.getSupplierID() + " and " + supplierID + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.supplierID': " + supplierRef.getSupplierID() + " and " + supplierID + "." );
				}
				if(supplierID != null)
					supplierRef.setSupplierID(supplierID);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (supplierRef.getAddress() != null && address != null && !supplierRef.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.address': " + supplierRef.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.address': " + supplierRef.getAddress() + " and " + address + "." );
				}
				if(address != null)
					supplierRef.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (supplierRef.getCity() != null && city != null && !supplierRef.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.city': " + supplierRef.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.city': " + supplierRef.getCity() + " and " + city + "." );
				}
				if(city != null)
					supplierRef.setCity(city);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (supplierRef.getCompanyName() != null && companyName != null && !supplierRef.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.companyName': " + supplierRef.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.companyName': " + supplierRef.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					supplierRef.setCompanyName(companyName);
				String contactName = Util.getStringValue(r.getAs("A_contactName"));
				if (supplierRef.getContactName() != null && contactName != null && !supplierRef.getContactName().equals(contactName)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.contactName': " + supplierRef.getContactName() + " and " + contactName + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.contactName': " + supplierRef.getContactName() + " and " + contactName + "." );
				}
				if(contactName != null)
					supplierRef.setContactName(contactName);
				String contactTitle = Util.getStringValue(r.getAs("A_contactTitle"));
				if (supplierRef.getContactTitle() != null && contactTitle != null && !supplierRef.getContactTitle().equals(contactTitle)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.contactTitle': " + supplierRef.getContactTitle() + " and " + contactTitle + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.contactTitle': " + supplierRef.getContactTitle() + " and " + contactTitle + "." );
				}
				if(contactTitle != null)
					supplierRef.setContactTitle(contactTitle);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (supplierRef.getCountry() != null && country != null && !supplierRef.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.country': " + supplierRef.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.country': " + supplierRef.getCountry() + " and " + country + "." );
				}
				if(country != null)
					supplierRef.setCountry(country);
				String fax = Util.getStringValue(r.getAs("A_fax"));
				if (supplierRef.getFax() != null && fax != null && !supplierRef.getFax().equals(fax)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.fax': " + supplierRef.getFax() + " and " + fax + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.fax': " + supplierRef.getFax() + " and " + fax + "." );
				}
				if(fax != null)
					supplierRef.setFax(fax);
				String homePage = Util.getStringValue(r.getAs("A_homePage"));
				if (supplierRef.getHomePage() != null && homePage != null && !supplierRef.getHomePage().equals(homePage)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.homePage': " + supplierRef.getHomePage() + " and " + homePage + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.homePage': " + supplierRef.getHomePage() + " and " + homePage + "." );
				}
				if(homePage != null)
					supplierRef.setHomePage(homePage);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (supplierRef.getPhone() != null && phone != null && !supplierRef.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.phone': " + supplierRef.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.phone': " + supplierRef.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					supplierRef.setPhone(phone);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (supplierRef.getPostalCode() != null && postalCode != null && !supplierRef.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.postalCode': " + supplierRef.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.postalCode': " + supplierRef.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					supplierRef.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (supplierRef.getRegion() != null && region != null && !supplierRef.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Supplies - different values found for attribute 'Supplies.region': " + supplierRef.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Supplies - different values found for attribute 'Supplies.region': " + supplierRef.getRegion() + " and " + region + "." );
				}
				if(region != null)
					supplierRef.setRegion(region);
	
				o = r.getAs("suppliedProduct");
				Product suppliedProduct = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						suppliedProduct.setProductID(Util.getIntegerValue(r2.getAs("productID")));
						suppliedProduct.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						suppliedProduct.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						suppliedProduct.setProductName(Util.getStringValue(r2.getAs("productName")));
						suppliedProduct.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						suppliedProduct.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						suppliedProduct.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						suppliedProduct.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Product) {
						suppliedProduct = (Product) o;
					}
				}
	
				res.setSuppliedProduct(suppliedProduct);
	
				return res;
		}, Encoders.bean(Supplies.class));
	
		
		
	}
	
	public static Dataset<Supplies> fullOuterJoinsSupplies(List<Dataset<Supplies>> datasetsPOJO) {
		return fullOuterJoinsSupplies(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Supplies> fullLeftOuterJoinsSupplies(List<Dataset<Supplies>> datasetsPOJO) {
		return fullOuterJoinsSupplies(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Supplies> fullOuterJoinsSupplies(List<Dataset<Supplies>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("suppliedProduct.productID");
	
		idFields.add("supplierRef.supplierID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Supplies> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("suppliedProduct_productID_" + i, d.col("suppliedProduct.productID"))
				.withColumn("supplierRef_supplierID_" + i, d.col("supplierRef.supplierID"))
				.withColumnRenamed("suppliedProduct", "suppliedProduct_" + i)
				.withColumnRenamed("supplierRef", "supplierRef_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("suppliedProduct_productID_0").equalTo(rows.get(1).col("suppliedProduct_productID_1"));
		joinCond = joinCond.and(rows.get(0).col("supplierRef_supplierID_0").equalTo(rows.get(1).col("supplierRef_supplierID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("suppliedProduct_productID_" + (i - 1)).equalTo(rows.get(i).col("suppliedProduct_productID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("supplierRef_supplierID_" + (i - 1)).equalTo(rows.get(i).col("supplierRef_supplierID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Supplies>) r -> {
				Supplies supplies_res = new Supplies();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							supplies_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							supplies_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Product suppliedProduct_res = new Product();
					Supplier supplierRef_res = new Supplier();
					
					// attribute 'Product.productID'
					Integer firstNotNull_suppliedProduct_productID = Util.getIntegerValue(r.getAs("suppliedProduct_0.productID"));
					suppliedProduct_res.setProductID(firstNotNull_suppliedProduct_productID);
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_suppliedProduct_unitsInStock = Util.getIntegerValue(r.getAs("suppliedProduct_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer suppliedProduct_unitsInStock2 = Util.getIntegerValue(r.getAs("suppliedProduct_" + i + ".unitsInStock"));
						if (firstNotNull_suppliedProduct_unitsInStock != null && suppliedProduct_unitsInStock2 != null && !firstNotNull_suppliedProduct_unitsInStock.equals(suppliedProduct_unitsInStock2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_suppliedProduct_unitsInStock + " and " + suppliedProduct_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_suppliedProduct_unitsInStock + " and " + suppliedProduct_unitsInStock2 + "." );
						}
						if (firstNotNull_suppliedProduct_unitsInStock == null && suppliedProduct_unitsInStock2 != null) {
							firstNotNull_suppliedProduct_unitsInStock = suppliedProduct_unitsInStock2;
						}
					}
					suppliedProduct_res.setUnitsInStock(firstNotNull_suppliedProduct_unitsInStock);
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_suppliedProduct_unitsOnOrder = Util.getIntegerValue(r.getAs("suppliedProduct_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer suppliedProduct_unitsOnOrder2 = Util.getIntegerValue(r.getAs("suppliedProduct_" + i + ".unitsOnOrder"));
						if (firstNotNull_suppliedProduct_unitsOnOrder != null && suppliedProduct_unitsOnOrder2 != null && !firstNotNull_suppliedProduct_unitsOnOrder.equals(suppliedProduct_unitsOnOrder2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_suppliedProduct_unitsOnOrder + " and " + suppliedProduct_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_suppliedProduct_unitsOnOrder + " and " + suppliedProduct_unitsOnOrder2 + "." );
						}
						if (firstNotNull_suppliedProduct_unitsOnOrder == null && suppliedProduct_unitsOnOrder2 != null) {
							firstNotNull_suppliedProduct_unitsOnOrder = suppliedProduct_unitsOnOrder2;
						}
					}
					suppliedProduct_res.setUnitsOnOrder(firstNotNull_suppliedProduct_unitsOnOrder);
					// attribute 'Product.productName'
					String firstNotNull_suppliedProduct_productName = Util.getStringValue(r.getAs("suppliedProduct_0.productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String suppliedProduct_productName2 = Util.getStringValue(r.getAs("suppliedProduct_" + i + ".productName"));
						if (firstNotNull_suppliedProduct_productName != null && suppliedProduct_productName2 != null && !firstNotNull_suppliedProduct_productName.equals(suppliedProduct_productName2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.productName': " + firstNotNull_suppliedProduct_productName + " and " + suppliedProduct_productName2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.productName': " + firstNotNull_suppliedProduct_productName + " and " + suppliedProduct_productName2 + "." );
						}
						if (firstNotNull_suppliedProduct_productName == null && suppliedProduct_productName2 != null) {
							firstNotNull_suppliedProduct_productName = suppliedProduct_productName2;
						}
					}
					suppliedProduct_res.setProductName(firstNotNull_suppliedProduct_productName);
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_suppliedProduct_quantityPerUnit = Util.getStringValue(r.getAs("suppliedProduct_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String suppliedProduct_quantityPerUnit2 = Util.getStringValue(r.getAs("suppliedProduct_" + i + ".quantityPerUnit"));
						if (firstNotNull_suppliedProduct_quantityPerUnit != null && suppliedProduct_quantityPerUnit2 != null && !firstNotNull_suppliedProduct_quantityPerUnit.equals(suppliedProduct_quantityPerUnit2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_suppliedProduct_quantityPerUnit + " and " + suppliedProduct_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_suppliedProduct_quantityPerUnit + " and " + suppliedProduct_quantityPerUnit2 + "." );
						}
						if (firstNotNull_suppliedProduct_quantityPerUnit == null && suppliedProduct_quantityPerUnit2 != null) {
							firstNotNull_suppliedProduct_quantityPerUnit = suppliedProduct_quantityPerUnit2;
						}
					}
					suppliedProduct_res.setQuantityPerUnit(firstNotNull_suppliedProduct_quantityPerUnit);
					// attribute 'Product.unitPrice'
					Double firstNotNull_suppliedProduct_unitPrice = Util.getDoubleValue(r.getAs("suppliedProduct_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double suppliedProduct_unitPrice2 = Util.getDoubleValue(r.getAs("suppliedProduct_" + i + ".unitPrice"));
						if (firstNotNull_suppliedProduct_unitPrice != null && suppliedProduct_unitPrice2 != null && !firstNotNull_suppliedProduct_unitPrice.equals(suppliedProduct_unitPrice2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_suppliedProduct_unitPrice + " and " + suppliedProduct_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_suppliedProduct_unitPrice + " and " + suppliedProduct_unitPrice2 + "." );
						}
						if (firstNotNull_suppliedProduct_unitPrice == null && suppliedProduct_unitPrice2 != null) {
							firstNotNull_suppliedProduct_unitPrice = suppliedProduct_unitPrice2;
						}
					}
					suppliedProduct_res.setUnitPrice(firstNotNull_suppliedProduct_unitPrice);
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_suppliedProduct_reorderLevel = Util.getIntegerValue(r.getAs("suppliedProduct_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer suppliedProduct_reorderLevel2 = Util.getIntegerValue(r.getAs("suppliedProduct_" + i + ".reorderLevel"));
						if (firstNotNull_suppliedProduct_reorderLevel != null && suppliedProduct_reorderLevel2 != null && !firstNotNull_suppliedProduct_reorderLevel.equals(suppliedProduct_reorderLevel2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_suppliedProduct_reorderLevel + " and " + suppliedProduct_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_suppliedProduct_reorderLevel + " and " + suppliedProduct_reorderLevel2 + "." );
						}
						if (firstNotNull_suppliedProduct_reorderLevel == null && suppliedProduct_reorderLevel2 != null) {
							firstNotNull_suppliedProduct_reorderLevel = suppliedProduct_reorderLevel2;
						}
					}
					suppliedProduct_res.setReorderLevel(firstNotNull_suppliedProduct_reorderLevel);
					// attribute 'Product.discontinued'
					Boolean firstNotNull_suppliedProduct_discontinued = Util.getBooleanValue(r.getAs("suppliedProduct_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean suppliedProduct_discontinued2 = Util.getBooleanValue(r.getAs("suppliedProduct_" + i + ".discontinued"));
						if (firstNotNull_suppliedProduct_discontinued != null && suppliedProduct_discontinued2 != null && !firstNotNull_suppliedProduct_discontinued.equals(suppliedProduct_discontinued2)) {
							supplies_res.addLogEvent("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_suppliedProduct_discontinued + " and " + suppliedProduct_discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+suppliedProduct_res.getProductID()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_suppliedProduct_discontinued + " and " + suppliedProduct_discontinued2 + "." );
						}
						if (firstNotNull_suppliedProduct_discontinued == null && suppliedProduct_discontinued2 != null) {
							firstNotNull_suppliedProduct_discontinued = suppliedProduct_discontinued2;
						}
					}
					suppliedProduct_res.setDiscontinued(firstNotNull_suppliedProduct_discontinued);
					// attribute 'Supplier.supplierID'
					Integer firstNotNull_supplierRef_supplierID = Util.getIntegerValue(r.getAs("supplierRef_0.supplierID"));
					supplierRef_res.setSupplierID(firstNotNull_supplierRef_supplierID);
					// attribute 'Supplier.address'
					String firstNotNull_supplierRef_address = Util.getStringValue(r.getAs("supplierRef_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_address2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".address"));
						if (firstNotNull_supplierRef_address != null && supplierRef_address2 != null && !firstNotNull_supplierRef_address.equals(supplierRef_address2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.address': " + firstNotNull_supplierRef_address + " and " + supplierRef_address2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.address': " + firstNotNull_supplierRef_address + " and " + supplierRef_address2 + "." );
						}
						if (firstNotNull_supplierRef_address == null && supplierRef_address2 != null) {
							firstNotNull_supplierRef_address = supplierRef_address2;
						}
					}
					supplierRef_res.setAddress(firstNotNull_supplierRef_address);
					// attribute 'Supplier.city'
					String firstNotNull_supplierRef_city = Util.getStringValue(r.getAs("supplierRef_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_city2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".city"));
						if (firstNotNull_supplierRef_city != null && supplierRef_city2 != null && !firstNotNull_supplierRef_city.equals(supplierRef_city2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.city': " + firstNotNull_supplierRef_city + " and " + supplierRef_city2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.city': " + firstNotNull_supplierRef_city + " and " + supplierRef_city2 + "." );
						}
						if (firstNotNull_supplierRef_city == null && supplierRef_city2 != null) {
							firstNotNull_supplierRef_city = supplierRef_city2;
						}
					}
					supplierRef_res.setCity(firstNotNull_supplierRef_city);
					// attribute 'Supplier.companyName'
					String firstNotNull_supplierRef_companyName = Util.getStringValue(r.getAs("supplierRef_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_companyName2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".companyName"));
						if (firstNotNull_supplierRef_companyName != null && supplierRef_companyName2 != null && !firstNotNull_supplierRef_companyName.equals(supplierRef_companyName2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.companyName': " + firstNotNull_supplierRef_companyName + " and " + supplierRef_companyName2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.companyName': " + firstNotNull_supplierRef_companyName + " and " + supplierRef_companyName2 + "." );
						}
						if (firstNotNull_supplierRef_companyName == null && supplierRef_companyName2 != null) {
							firstNotNull_supplierRef_companyName = supplierRef_companyName2;
						}
					}
					supplierRef_res.setCompanyName(firstNotNull_supplierRef_companyName);
					// attribute 'Supplier.contactName'
					String firstNotNull_supplierRef_contactName = Util.getStringValue(r.getAs("supplierRef_0.contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_contactName2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".contactName"));
						if (firstNotNull_supplierRef_contactName != null && supplierRef_contactName2 != null && !firstNotNull_supplierRef_contactName.equals(supplierRef_contactName2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.contactName': " + firstNotNull_supplierRef_contactName + " and " + supplierRef_contactName2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.contactName': " + firstNotNull_supplierRef_contactName + " and " + supplierRef_contactName2 + "." );
						}
						if (firstNotNull_supplierRef_contactName == null && supplierRef_contactName2 != null) {
							firstNotNull_supplierRef_contactName = supplierRef_contactName2;
						}
					}
					supplierRef_res.setContactName(firstNotNull_supplierRef_contactName);
					// attribute 'Supplier.contactTitle'
					String firstNotNull_supplierRef_contactTitle = Util.getStringValue(r.getAs("supplierRef_0.contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_contactTitle2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".contactTitle"));
						if (firstNotNull_supplierRef_contactTitle != null && supplierRef_contactTitle2 != null && !firstNotNull_supplierRef_contactTitle.equals(supplierRef_contactTitle2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.contactTitle': " + firstNotNull_supplierRef_contactTitle + " and " + supplierRef_contactTitle2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.contactTitle': " + firstNotNull_supplierRef_contactTitle + " and " + supplierRef_contactTitle2 + "." );
						}
						if (firstNotNull_supplierRef_contactTitle == null && supplierRef_contactTitle2 != null) {
							firstNotNull_supplierRef_contactTitle = supplierRef_contactTitle2;
						}
					}
					supplierRef_res.setContactTitle(firstNotNull_supplierRef_contactTitle);
					// attribute 'Supplier.country'
					String firstNotNull_supplierRef_country = Util.getStringValue(r.getAs("supplierRef_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_country2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".country"));
						if (firstNotNull_supplierRef_country != null && supplierRef_country2 != null && !firstNotNull_supplierRef_country.equals(supplierRef_country2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.country': " + firstNotNull_supplierRef_country + " and " + supplierRef_country2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.country': " + firstNotNull_supplierRef_country + " and " + supplierRef_country2 + "." );
						}
						if (firstNotNull_supplierRef_country == null && supplierRef_country2 != null) {
							firstNotNull_supplierRef_country = supplierRef_country2;
						}
					}
					supplierRef_res.setCountry(firstNotNull_supplierRef_country);
					// attribute 'Supplier.fax'
					String firstNotNull_supplierRef_fax = Util.getStringValue(r.getAs("supplierRef_0.fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_fax2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".fax"));
						if (firstNotNull_supplierRef_fax != null && supplierRef_fax2 != null && !firstNotNull_supplierRef_fax.equals(supplierRef_fax2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.fax': " + firstNotNull_supplierRef_fax + " and " + supplierRef_fax2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.fax': " + firstNotNull_supplierRef_fax + " and " + supplierRef_fax2 + "." );
						}
						if (firstNotNull_supplierRef_fax == null && supplierRef_fax2 != null) {
							firstNotNull_supplierRef_fax = supplierRef_fax2;
						}
					}
					supplierRef_res.setFax(firstNotNull_supplierRef_fax);
					// attribute 'Supplier.homePage'
					String firstNotNull_supplierRef_homePage = Util.getStringValue(r.getAs("supplierRef_0.homePage"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_homePage2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".homePage"));
						if (firstNotNull_supplierRef_homePage != null && supplierRef_homePage2 != null && !firstNotNull_supplierRef_homePage.equals(supplierRef_homePage2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.homePage': " + firstNotNull_supplierRef_homePage + " and " + supplierRef_homePage2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.homePage': " + firstNotNull_supplierRef_homePage + " and " + supplierRef_homePage2 + "." );
						}
						if (firstNotNull_supplierRef_homePage == null && supplierRef_homePage2 != null) {
							firstNotNull_supplierRef_homePage = supplierRef_homePage2;
						}
					}
					supplierRef_res.setHomePage(firstNotNull_supplierRef_homePage);
					// attribute 'Supplier.phone'
					String firstNotNull_supplierRef_phone = Util.getStringValue(r.getAs("supplierRef_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_phone2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".phone"));
						if (firstNotNull_supplierRef_phone != null && supplierRef_phone2 != null && !firstNotNull_supplierRef_phone.equals(supplierRef_phone2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.phone': " + firstNotNull_supplierRef_phone + " and " + supplierRef_phone2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.phone': " + firstNotNull_supplierRef_phone + " and " + supplierRef_phone2 + "." );
						}
						if (firstNotNull_supplierRef_phone == null && supplierRef_phone2 != null) {
							firstNotNull_supplierRef_phone = supplierRef_phone2;
						}
					}
					supplierRef_res.setPhone(firstNotNull_supplierRef_phone);
					// attribute 'Supplier.postalCode'
					String firstNotNull_supplierRef_postalCode = Util.getStringValue(r.getAs("supplierRef_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_postalCode2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".postalCode"));
						if (firstNotNull_supplierRef_postalCode != null && supplierRef_postalCode2 != null && !firstNotNull_supplierRef_postalCode.equals(supplierRef_postalCode2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.postalCode': " + firstNotNull_supplierRef_postalCode + " and " + supplierRef_postalCode2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.postalCode': " + firstNotNull_supplierRef_postalCode + " and " + supplierRef_postalCode2 + "." );
						}
						if (firstNotNull_supplierRef_postalCode == null && supplierRef_postalCode2 != null) {
							firstNotNull_supplierRef_postalCode = supplierRef_postalCode2;
						}
					}
					supplierRef_res.setPostalCode(firstNotNull_supplierRef_postalCode);
					// attribute 'Supplier.region'
					String firstNotNull_supplierRef_region = Util.getStringValue(r.getAs("supplierRef_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String supplierRef_region2 = Util.getStringValue(r.getAs("supplierRef_" + i + ".region"));
						if (firstNotNull_supplierRef_region != null && supplierRef_region2 != null && !firstNotNull_supplierRef_region.equals(supplierRef_region2)) {
							supplies_res.addLogEvent("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.region': " + firstNotNull_supplierRef_region + " and " + supplierRef_region2 + "." );
							logger.warn("Data consistency problem for [Supplier - id :"+supplierRef_res.getSupplierID()+"]: different values found for attribute 'Supplier.region': " + firstNotNull_supplierRef_region + " and " + supplierRef_region2 + "." );
						}
						if (firstNotNull_supplierRef_region == null && supplierRef_region2 != null) {
							firstNotNull_supplierRef_region = supplierRef_region2;
						}
					}
					supplierRef_res.setRegion(firstNotNull_supplierRef_region);
	
					supplies_res.setSuppliedProduct(suppliedProduct_res);
					supplies_res.setSupplierRef(supplierRef_res);
					return supplies_res;
		}
		, Encoders.bean(Supplies.class));
	
	}
	
	//Empty arguments
	public Dataset<Supplies> getSuppliesList(){
		 return getSuppliesList(null,null);
	}
	
	public abstract Dataset<Supplies> getSuppliesList(
		Condition<ProductAttribute> suppliedProduct_condition,
		Condition<SupplierAttribute> supplierRef_condition);
	
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
	
	
	
	public abstract void deleteSuppliesList(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition);
	
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
