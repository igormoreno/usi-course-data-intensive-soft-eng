package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Product;
import conditions.*;
import dao.services.ProductService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Dataset;
import org.apache.spark.sql.Encoders;
import util.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang3.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class ProductServiceImpl extends ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromReldata(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInProductsInfoFromReldataWithTableAlias(condition, refilterFlag, "");
	}
	
	public static List<String> getSQLSetClauseInProductsInfoFromReldata(conditions.SetClause<ProductAttribute> set) {
		List<String> res = new ArrayList<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ProductAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ProductAttribute, Object> e : clause.entrySet()) {
				ProductAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ProductAttribute.productID ) {
					res.add("ProductID = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.productName ) {
					res.add("ProductName = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.quantityPerUnit ) {
					res.add("QuantityPerUnit = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.unitPrice ) {
					res.add("UnitPrice = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.reorderLevel ) {
					res.add("ReorderLevel = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.discontinued ) {
					res.add("Discontinued = " + Util.getDelimitedSQLValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return res;
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductsInfoFromReldataWithTableAlias(Condition<ProductAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductAttribute.productID ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "ProductID " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.productName ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "ProductName " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.quantityPerUnit ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "QuantityPerUnit " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.unitPrice ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "UnitPrice " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.reorderLevel ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "ReorderLevel " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.discontinued ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						Class cl = null;
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
							cl = String.class;
						} else
							cl = value.getClass();
						
						where = tableAlias + "Discontinued " + sqlOp + " ?";
	
						preparedValue = Util.getDelimitedSQLValue(cl, preparedValue);
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				} else {
					if(attr == ProductAttribute.productID ) {
						if(op == Operator.EQUALS)
							where =  "ProductID IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductID IS NOT NULL";
					}
					if(attr == ProductAttribute.productName ) {
						if(op == Operator.EQUALS)
							where =  "ProductName IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ProductName IS NOT NULL";
					}
					if(attr == ProductAttribute.quantityPerUnit ) {
						if(op == Operator.EQUALS)
							where =  "QuantityPerUnit IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "QuantityPerUnit IS NOT NULL";
					}
					if(attr == ProductAttribute.unitPrice ) {
						if(op == Operator.EQUALS)
							where =  "UnitPrice IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "UnitPrice IS NOT NULL";
					}
					if(attr == ProductAttribute.reorderLevel ) {
						if(op == Operator.EQUALS)
							where =  "ReorderLevel IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "ReorderLevel IS NOT NULL";
					}
					if(attr == ProductAttribute.discontinued ) {
						if(op == Operator.EQUALS)
							where =  "Discontinued IS NULL";
						if(op == Operator.NOT_EQUALS)
							where =  "Discontinued IS NOT NULL";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductsInfoFromReldata(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductsInfoFromReldata(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductsInfoFromReldata(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductsInfoFromReldata(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	
	public Dataset<Product> getProductListInProductsInfoFromReldata(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductsInfoFromReldata(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", preparedValue);
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("reldata", "ProductsInfo", where);
		
	
		Dataset<Product> res = d.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
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
	
	
	
					return product_res;
				}, Encoders.bean(Product.class));
	
	
		return res;
		
	}
	
	
	
	
	//TODO redis
	public Dataset<Product> getProductListInProductStockInfoFromMyRedis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductAttribute.productID));
			keyAttributes.add(ProductAttribute.productID);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("ProductID");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":STOCKINFO");
		keypatternAllVariables=keypatternAllVariables.concat(":STOCKINFO");
		if(!refilterFlag.booleanValue()){
			Set<ProductAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
	
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		StructType structType = new StructType(new StructField[] {
			DataTypes.createStructField("_id", DataTypes.StringType, true), //technical field to store the key.
			DataTypes.createStructField("UnitsInStock", DataTypes.StringType, true)
	,		DataTypes.createStructField("UnitsOnOrder", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myRedis",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = false;
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<Product> res = rows.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					String key = isStriped ? prefix + r.getAs("_id") : r.getAs("_id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					// attribute [Product.ProductID]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("ProductID")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Product' mapped physical field 'ProductID' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String productID = null;
					if(matches) {
						productID = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for ProductproductID attribute stored in db myRedis. Regex [{}] Value [{}]",regex,value);
						product_res.addLogEvent("Cannot retrieve value for Product.productID attribute stored in db myRedis. Probably due to an ambiguous regex.");
					}
					product_res.setProductID(productID == null ? null : Integer.parseInt(productID));
					// attribute [Product.UnitsInStock]
					Integer unitsInStock = r.getAs("UnitsInStock") == null ? null : Integer.parseInt(r.getAs("UnitsInStock"));
					product_res.setUnitsInStock(unitsInStock);
					// attribute [Product.UnitsOnOrder]
					Integer unitsOnOrder = r.getAs("UnitsOnOrder") == null ? null : Integer.parseInt(r.getAs("UnitsOnOrder"));
					product_res.setUnitsOnOrder(unitsOnOrder);
	
						return product_res;
				}, Encoders.bean(Product.class));
		res=res.dropDuplicates(new String[] {"productID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Product> getProductRefListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderRef_condition,conditions.Condition<conditions.ProductAttribute> productRef_condition, conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition)		{
		MutableBoolean productRef_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean orderRef_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		
		//join between 2 SQL tables and a non-relational structure
		// (A - AB) (B)
		orderRef_refilter = new MutableBoolean(false);
		MutableBoolean composed_of_refilter = new MutableBoolean(false);
		Dataset<Composed_ofTDO> res_composed_of_productRef_orderRef = composed_ofService.getComposed_ofTDOListInProductsInfoAndOrder_DetailsFromreldata(productRef_condition, composed_of_condition, productRef_refilter, composed_of_refilter);
		Dataset<OrderTDO> res_orderRef_productRef = composed_ofService.getOrderTDOListOrderRefInOrderRefInOrdersFromMyMongoDB(orderRef_condition, orderRef_refilter);
		if(orderRef_refilter.booleanValue()) {
			if(all == null)
					all = new OrderServiceImpl().getOrderList(orderRef_condition);
			joinCondition = null;
				joinCondition = res_orderRef_productRef.col("orderID").equalTo(all.col("orderID"));
				res_orderRef_productRef = res_orderRef_productRef.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		
		Dataset<Row> res_row_productRef_orderRef = res_composed_of_productRef_orderRef.join(res_orderRef_productRef.withColumnRenamed("logEvents", "composed_of_logEvents"),
																														res_composed_of_productRef_orderRef.col("reldata_Order_Details_orderRef_source_OrderRef").equalTo(res_orderRef_productRef.col("reldata_Order_Details_orderRef_target_OrderID")));																												
																														
		Dataset<Product> res_Product_productRef = res_row_productRef_orderRef.select("productRef.*").as(Encoders.bean(Product.class));
		datasetsPOJO.add(res_Product_productRef.dropDuplicates(new String[] {"productID"}));	
		
		
		
		Dataset<Composed_of> res_composed_of_productRef;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInProductStockInfoFromMyRedis(productRef_condition, new MutableBoolean(false)));
		Dataset<Product> lonelyProduct = fullOuterJoinsProduct(lonelyProductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinsProduct(Arrays.asList(res, lonelyProduct));
		}
		if(productRef_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> productRef_condition == null || productRef_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Product> getSuppliedProductListInSupplies(conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,conditions.Condition<conditions.SupplierAttribute> supplierRef_condition)		{
		MutableBoolean suppliedProduct_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Supplier> all = null;
		boolean all_already_persisted = false;
		MutableBoolean supplierRef_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'suppliedProduct' in reference 'supplierRef'. A->B Scenario
		supplierRef_refilter = new MutableBoolean(false);
		Dataset<ProductTDO> productTDOsupplierRefsuppliedProduct = suppliesService.getProductTDOListSuppliedProductInSupplierRefInProductsInfoFromReldata(suppliedProduct_condition, suppliedProduct_refilter);
		Dataset<SupplierTDO> supplierTDOsupplierRefsupplierRef = suppliesService.getSupplierTDOListSupplierRefInSupplierRefInProductsInfoFromReldata(supplierRef_condition, supplierRef_refilter);
		if(supplierRef_refilter.booleanValue()) {
			if(all == null)
				all = new SupplierServiceImpl().getSupplierList(supplierRef_condition);
			joinCondition = null;
			joinCondition = supplierTDOsupplierRefsupplierRef.col("supplierID").equalTo(all.col("supplierID"));
			if(joinCondition == null)
				supplierTDOsupplierRefsupplierRef = supplierTDOsupplierRefsupplierRef.as("A").join(all).select("A.*").as(Encoders.bean(SupplierTDO.class));
			else
				supplierTDOsupplierRefsupplierRef = supplierTDOsupplierRefsupplierRef.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(SupplierTDO.class));
		}
	
		
		Dataset<Row> res_supplierRef = productTDOsupplierRefsuppliedProduct.join(supplierTDOsupplierRefsupplierRef
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
		Dataset<Product> res_Product_supplierRef = res_supplierRef.select( "productID", "unitsInStock", "unitsOnOrder", "productName", "quantityPerUnit", "unitPrice", "reorderLevel", "discontinued", "logEvents").as(Encoders.bean(Product.class));
		
		res_Product_supplierRef = res_Product_supplierRef.dropDuplicates(new String[] {"productID"});
		datasetsPOJO.add(res_Product_supplierRef);
		
		
		Dataset<Supplies> res_supplies_suppliedProduct;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInProductStockInfoFromMyRedis(suppliedProduct_condition, new MutableBoolean(false)));
		Dataset<Product> lonelyProduct = fullOuterJoinsProduct(lonelyProductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinsProduct(Arrays.asList(res, lonelyProduct));
		}
		if(suppliedProduct_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> suppliedProduct_condition == null || suppliedProduct_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertProduct(
		Product product,
		Supplier	supplierRefSupplies){
			boolean inserted = false;
			// Insert in standalone structures
			inserted = insertProductInProductStockInfoFromMyRedis(product)|| inserted ;
			// Insert in structures containing double embedded role
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			inserted = insertProductInProductsInfoFromReldata(product,supplierRefSupplies)|| inserted ;
			// Insert in ref structures mapped to opposite role of mandatory role  
			return inserted;
		}
	
	public boolean insertProductInProductStockInfoFromMyRedis(Product product)	{
		String idvalue="";
		idvalue+=product.getProductID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "PRODUCT:";
			key += product.getProductID();
			key += ":STOCKINFO";
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_UnitsInStock="UnitsInStock";
			String _value_UnitsInStock="";
			if(product.getUnitsInStock()!=null){
				toAdd = true;
				_value_UnitsInStock += product.getUnitsInStock();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsInStock,_value_UnitsInStock));
			toAdd = false;
			String _fieldname_UnitsOnOrder="UnitsOnOrder";
			String _value_UnitsOnOrder="";
			if(product.getUnitsOnOrder()!=null){
				toAdd = true;
				_value_UnitsOnOrder += product.getUnitsOnOrder();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitsOnOrder,_value_UnitsOnOrder));
			
			
			
			SparkConnectionMgr.writeKeyValueHash(key,hash, "myRedis");
	
			logger.info("Inserted [Product] entity ID [{}] in [ProductStockInfo] in database [MyRedis]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [ProductStockInfo] in database [MyRedis]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertProductInProductsInfoFromReldata(Product product,
		Supplier	supplierRefSupplies)	{
			 // Implement Insert in structures with mandatory references
			List<String> columns = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			List<List<Object>> rows = new ArrayList<>();
			Object productId;
		columns.add("ProductID");
		values.add(product.getProductID());
		columns.add("ProductName");
		values.add(product.getProductName());
		columns.add("QuantityPerUnit");
		values.add(product.getQuantityPerUnit());
		columns.add("UnitPrice");
		values.add(product.getUnitPrice());
		columns.add("ReorderLevel");
		values.add(product.getReorderLevel());
		columns.add("Discontinued");
		values.add(product.getDiscontinued());
			// Ref 'supplierRef' mapped to role 'suppliedProduct'
			columns.add("SupplierRef");
			values.add(supplierRefSupplies.getSupplierID());
			rows.add(values);
			DBConnectionMgr.insertInTable(columns, rows, "ProductsInfo", "reldata");
			return true;
		
		}
	private boolean inUpdateMethod = false;
	private List<Row> allProductIdList = null;
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInProductStockInfoFromMyRedis = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInProductStockInfoFromMyRedis.booleanValue())
				updateProductListInProductStockInfoFromMyRedis(condition, set);
		
	
			if(!refilterInProductStockInfoFromMyRedis.booleanValue())
				updateProductListInProductStockInfoFromMyRedis(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateProductListInProductStockInfoFromMyRedis(Condition<ProductAttribute> condition, SetClause<ProductAttribute> set) {
		//TODO
	}
	
	
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void updateProductRefListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
	public void updateProductRefListInComposed_ofByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductRefListInComposed_of(orderRef_condition, null, null, set);
	}
	
	public void updateProductRefListInComposed_ofByOrderRef(
		pojo.Order orderRef,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateProductRefListInComposed_ofByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductRefListInComposed_of(null, productRef_condition, null, set);
	}
	public void updateProductRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductRefListInComposed_of(null, null, composed_of_condition, set);
	}
	public void updateSuppliedProductListInSupplies(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
	public void updateSuppliedProductListInSuppliesBySuppliedProductCondition(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateSuppliedProductListInSupplies(suppliedProduct_condition, null, set);
	}
	public void updateSuppliedProductListInSuppliesBySupplierRefCondition(
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateSuppliedProductListInSupplies(null, supplierRef_condition, set);
	}
	
	public void updateSuppliedProductListInSuppliesBySupplierRef(
		pojo.Supplier supplierRef,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition){
		//TODO
	}
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void deleteProductRefListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,	
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of){
			//TODO
		}
	
	public void deleteProductRefListInComposed_ofByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition
	){
		deleteProductRefListInComposed_of(orderRef_condition, null, null);
	}
	
	public void deleteProductRefListInComposed_ofByOrderRef(
		pojo.Order orderRef 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteProductRefListInComposed_ofByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition
	){
		deleteProductRefListInComposed_of(null, productRef_condition, null);
	}
	public void deleteProductRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		deleteProductRefListInComposed_of(null, null, composed_of_condition);
	}
	public void deleteSuppliedProductListInSupplies(	
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,	
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition){
			//TODO
		}
	
	public void deleteSuppliedProductListInSuppliesBySuppliedProductCondition(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition
	){
		deleteSuppliedProductListInSupplies(suppliedProduct_condition, null);
	}
	public void deleteSuppliedProductListInSuppliesBySupplierRefCondition(
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition
	){
		deleteSuppliedProductListInSupplies(null, supplierRef_condition);
	}
	
	public void deleteSuppliedProductListInSuppliesBySupplierRef(
		pojo.Supplier supplierRef 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
