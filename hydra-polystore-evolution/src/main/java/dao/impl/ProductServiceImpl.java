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
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInOrdersFromMyMongoDB(conditions.SetClause<ProductAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<ProductAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<ProductAttribute, Object> e : clause.entrySet()) {
				ProductAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == ProductAttribute.productID ) {
					String fieldName = "ProductID";
					fieldName = "products.$[products0]." + fieldName;
					arrayFields.add("products0");
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == ProductAttribute.productName ) {
					String fieldName = "ProductName";
					fieldName = "products.$[products0]." + fieldName;
					arrayFields.add("products0");
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
			}
	
			for(java.util.Map.Entry<String, java.util.Map<String, String>> entry : longFieldValues.entrySet()) {
				String longField = entry.getKey();
				java.util.Map<String, String> values = entry.getValue();
			}
	
		}
		return new ImmutablePair<List<String>, List<String>>(res, new ArrayList<String>(arrayFields));
	}
	
	public static String getBSONMatchQueryInOrdersFromMyMongoDB(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ProductAttribute.productID ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ProductID': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "products." + res;
					res = "'" + res;
					}
					if(attr == ProductAttribute.productName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ProductName': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "products." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(Condition<ProductAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ProductAttribute.productID ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ProductID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							if(arrayVariableNames.contains("products0")) {
								arrayVar = true;
								arrayVariablesUsed.add("products0");
								bson = "products0." + bson; 
							} else {
								bson = "products." + bson;
							}
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == ProductAttribute.productName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ProductName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
						if(!arrayVar) {
							if(arrayVariableNames.contains("products0")) {
								arrayVar = true;
								arrayVariablesUsed.add("products0");
								bson = "products0." + bson; 
							} else {
								bson = "products." + bson;
							}
						}
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
					}
					
				}
	
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $and: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight);
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
				String queryLeft = bsonLeft.getLeft();
				String queryRight = bsonRight.getLeft();
				List<String> arrayFilterLeft = bsonLeft.getRight();
				List<String> arrayFilterRight = bsonRight.getRight();
	
				if(queryLeft == null && queryRight != null)
					query = queryRight;
				if(queryLeft != null && queryRight == null)
					query = queryLeft;
				if(queryLeft != null && queryRight != null)
					query = " $or: [ {" + queryLeft + "}, {" + queryRight + "}] ";
	
				arrayFilters.addAll(arrayFilterLeft);
				arrayFilters.addAll(arrayFilterRight); // can be a problem
			}
		}
	
		return new ImmutablePair<String, List<String>>(query, arrayFilters);
	}
	
	
	
	public Dataset<Product> getProductListInOrdersFromMyMongoDB(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ProductServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<Product> res = dataset.flatMap((FlatMapFunction<Row, Product>) r -> {
				Set<Product> list_res = new HashSet<Product>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Product product1 = new Product();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("products");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Product product2 = (Product) product1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Product.productID for field ProductID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ProductID")) {
								if(nestedRow.getAs("ProductID")==null)
									product2.setProductID(null);
								else{
									product2.setProductID(Util.getIntegerValue(nestedRow.getAs("ProductID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.productName for field ProductName			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ProductName")) {
								if(nestedRow.getAs("ProductName")==null)
									product2.setProductName(null);
								else{
									product2.setProductName(Util.getStringValue(nestedRow.getAs("ProductName")));
									toAdd2 = true;					
									}
							}
							if(toAdd2&& (condition ==null || refilterFlag.booleanValue() || condition.evaluate(product2))) {
								list_res.add(product2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						list_res.add(product1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Product.class));
		res= res.dropDuplicates(new String[]{"productID"});
		return res;
		
	}
	
	
	
	
	//TODO redis
	public Dataset<Product> getProductListInProductFromMyRedis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
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
			DataTypes.createStructField("ProductName", DataTypes.StringType, true)
	,		DataTypes.createStructField("QuantityPerUnit", DataTypes.StringType, true)
	,		DataTypes.createStructField("UnitPrice", DataTypes.StringType, true)
	,		DataTypes.createStructField("ReorderLevel", DataTypes.StringType, true)
	,		DataTypes.createStructField("Discontinued", DataTypes.StringType, true)
	,		DataTypes.createStructField("UnitsInStock", DataTypes.StringType, true)
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
					// attribute [Product.ProductName]
					String productName = r.getAs("ProductName") == null ? null : r.getAs("ProductName");
					product_res.setProductName(productName);
					// attribute [Product.QuantityPerUnit]
					String quantityPerUnit = r.getAs("QuantityPerUnit") == null ? null : r.getAs("QuantityPerUnit");
					product_res.setQuantityPerUnit(quantityPerUnit);
					// attribute [Product.UnitPrice]
					Double unitPrice = r.getAs("UnitPrice") == null ? null : Double.parseDouble(r.getAs("UnitPrice"));
					product_res.setUnitPrice(unitPrice);
					// attribute [Product.ReorderLevel]
					Integer reorderLevel = r.getAs("ReorderLevel") == null ? null : Integer.parseInt(r.getAs("ReorderLevel"));
					product_res.setReorderLevel(reorderLevel);
					// attribute [Product.Discontinued]
					Boolean discontinued = r.getAs("Discontinued") == null ? null : Boolean.parseBoolean(r.getAs("Discontinued"));
					product_res.setDiscontinued(discontinued);
	
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
		
		
		Dataset<Composed_of> res_composed_of_productRef;
		Dataset<Product> res_Product;
		// Role 'productRef' mapped to EmbeddedObject 'products' - 'Order' containing 'Product'
		orderRef_refilter = new MutableBoolean(false);
		res_composed_of_productRef = composed_ofService.getComposed_ofListInmyMongoDBOrdersproducts(productRef_condition, orderRef_condition, productRef_refilter, orderRef_refilter);
		if(orderRef_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(orderRef_condition);
			joinCondition = null;
			joinCondition = res_composed_of_productRef.col("orderRef.orderID").equalTo(all.col("orderID"));
			if(joinCondition == null)
				res_Product = res_composed_of_productRef.join(all).select("productRef.*").as(Encoders.bean(Product.class));
			else
				res_Product = res_composed_of_productRef.join(all, joinCondition).select("productRef.*").as(Encoders.bean(Product.class));
		
		} else
			res_Product = res_composed_of_productRef.map((MapFunction<Composed_of,Product>) r -> r.getProductRef(), Encoders.bean(Product.class));
		res_Product = res_Product.dropDuplicates(new String[] {"productID"});
		datasetsPOJO.add(res_Product);
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInProductFromMyRedis(productRef_condition, new MutableBoolean(false)));
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
		
		supplierRef_refilter = new MutableBoolean(false);
		// For role 'supplierRef' in reference 'prodSupplied'  B->A Scenario
		Dataset<SupplierTDO> supplierTDOprodSuppliedsupplierRef = suppliesService.getSupplierTDOListSupplierRefInProdSuppliedInSuppliersFromMyMongoDB(supplierRef_condition, supplierRef_refilter);
		Dataset<ProductTDO> productTDOprodSuppliedsuppliedProduct = suppliesService.getProductTDOListSuppliedProductInProdSuppliedInSuppliersFromMyMongoDB(suppliedProduct_condition, suppliedProduct_refilter);
		if(supplierRef_refilter.booleanValue()) {
			if(all == null)
				all = new SupplierServiceImpl().getSupplierList(supplierRef_condition);
			joinCondition = null;
			joinCondition = supplierTDOprodSuppliedsupplierRef.col("supplierID").equalTo(all.col("supplierID"));
			if(joinCondition == null)
				supplierTDOprodSuppliedsupplierRef = supplierTDOprodSuppliedsupplierRef.as("A").join(all).select("A.*").as(Encoders.bean(SupplierTDO.class));
			else
				supplierTDOprodSuppliedsupplierRef = supplierTDOprodSuppliedsupplierRef.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(SupplierTDO.class));
		}
		// Multi valued reference
		Dataset<Row> res_prodSupplied = 
			supplierTDOprodSuppliedsupplierRef
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
			.withColumnRenamed("logEvents", "Supplier_logEvents")
			.join(productTDOprodSuppliedsuppliedProduct,
				functions.array_contains(supplierTDOprodSuppliedsupplierRef.col("myMongoDB_Suppliers_prodSupplied_source_products"),productTDOprodSuppliedsuppliedProduct.col("myMongoDB_Suppliers_prodSupplied_target_ProductID")));
		Dataset<Product> res_Product_prodSupplied = res_prodSupplied.select( "productID", "unitsInStock", "unitsOnOrder", "productName", "quantityPerUnit", "unitPrice", "reorderLevel", "discontinued", "logEvents").as(Encoders.bean(Product.class));
		res_Product_prodSupplied = res_Product_prodSupplied.dropDuplicates(new String[] {"productID"});
		datasetsPOJO.add(res_Product_prodSupplied);
		
		Dataset<Supplies> res_supplies_suppliedProduct;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInOrdersFromMyMongoDB(suppliedProduct_condition, new MutableBoolean(false)));
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
			inserted = insertProductInProductFromMyRedis(product)|| inserted ;
			// Insert in structures containing double embedded role
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			// Insert in ref structures mapped to opposite role of mandatory role  
			inserted = insertProductInSuppliersFromMyMongoDB(product,supplierRefSupplies)|| inserted ;
			return inserted;
		}
	
	public boolean insertProductInProductFromMyRedis(Product product)	{
		String idvalue="";
		idvalue+=product.getProductID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
			String key="";
			key += "PRODUCT:";
			key += product.getProductID();
			// Generate for hash value
			boolean toAdd = false;
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String _fieldname_ProductName="ProductName";
			String _value_ProductName="";
			if(product.getProductName()!=null){
				toAdd = true;
				_value_ProductName += product.getProductName();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_ProductName,_value_ProductName));
			toAdd = false;
			String _fieldname_QuantityPerUnit="QuantityPerUnit";
			String _value_QuantityPerUnit="";
			if(product.getQuantityPerUnit()!=null){
				toAdd = true;
				_value_QuantityPerUnit += product.getQuantityPerUnit();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_QuantityPerUnit,_value_QuantityPerUnit));
			toAdd = false;
			String _fieldname_UnitPrice="UnitPrice";
			String _value_UnitPrice="";
			if(product.getUnitPrice()!=null){
				toAdd = true;
				_value_UnitPrice += product.getUnitPrice();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_UnitPrice,_value_UnitPrice));
			toAdd = false;
			String _fieldname_ReorderLevel="ReorderLevel";
			String _value_ReorderLevel="";
			if(product.getReorderLevel()!=null){
				toAdd = true;
				_value_ReorderLevel += product.getReorderLevel();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_ReorderLevel,_value_ReorderLevel));
			toAdd = false;
			String _fieldname_Discontinued="Discontinued";
			String _value_Discontinued="";
			if(product.getDiscontinued()!=null){
				toAdd = true;
				_value_Discontinued += product.getDiscontinued();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(_fieldname_Discontinued,_value_Discontinued));
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
	
			logger.info("Inserted [Product] entity ID [{}] in [Product] in database [MyRedis]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [Product] in database [MyRedis]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertProductInSuppliersFromMyMongoDB(Product product,
		Supplier	supplierRefSupplies)	{
		 	// Entity 'Product' reference in structure 'Suppliers'
			// In document db
			Bson filter;
			Bson updateOp;
			updateOp = addToSet("products",product.getProductID());
			filter = eq("SupplierID",supplierRefSupplies.getSupplierID());
			DBConnectionMgr.update(filter, updateOp, "Suppliers", "myMongoDB");
			return true;
			
		}
	private boolean inUpdateMethod = false;
	private List<Row> allProductIdList = null;
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInProductFromMyRedis = new MutableBoolean(false);
			//TODO
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInProductFromMyRedis.booleanValue())
				updateProductListInProductFromMyRedis(condition, set);
		
	
			if(!refilterInProductFromMyRedis.booleanValue())
				updateProductListInProductFromMyRedis(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateProductListInProductFromMyRedis(Condition<ProductAttribute> condition, SetClause<ProductAttribute> set) {
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
