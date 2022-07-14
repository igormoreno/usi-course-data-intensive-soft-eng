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
import conditions.Composed_ofAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrderTDO;
import tdo.Composed_ofTDO;
import conditions.OrderAttribute;
import dao.services.OrderService;
import tdo.ProductTDO;
import tdo.Composed_ofTDO;
import conditions.ProductAttribute;
import dao.services.ProductService;
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

public class Composed_ofServiceImpl extends dao.services.Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofServiceImpl.class);
	
	
	// method accessing the embedded object products mapped to role productRef
	public Dataset<Composed_of> getComposed_ofListInmyMongoDBOrdersproducts(Condition<ProductAttribute> productRef_condition, Condition<OrderAttribute> orderRef_condition, MutableBoolean productRef_refilter, MutableBoolean orderRef_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(orderRef_condition ,orderRef_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = ProductServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(productRef_condition ,productRef_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
		
			Dataset<Composed_of> res = dataset.flatMap((FlatMapFunction<Row, Composed_of>) r -> {
					List<Composed_of> list_res = new ArrayList<Composed_of>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Composed_of composed_of1 = new Composed_of();
					composed_of1.setOrderRef(new Order());
					composed_of1.setProductRef(new Product());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.orderID for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							composed_of1.getOrderRef().setOrderID(null);
						else{
							composed_of1.getOrderRef().setOrderID(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							composed_of1.getOrderRef().setFreight(null);
						else{
							composed_of1.getOrderRef().setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							composed_of1.getOrderRef().setOrderDate(null);
						else{
							composed_of1.getOrderRef().setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							composed_of1.getOrderRef().setRequiredDate(null);
						else{
							composed_of1.getOrderRef().setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							composed_of1.getOrderRef().setShipAddress(null);
						else{
							composed_of1.getOrderRef().setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							composed_of1.getOrderRef().setShipCity(null);
						else{
							composed_of1.getOrderRef().setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							composed_of1.getOrderRef().setShipCountry(null);
						else{
							composed_of1.getOrderRef().setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							composed_of1.getOrderRef().setShipName(null);
						else{
							composed_of1.getOrderRef().setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							composed_of1.getOrderRef().setShipPostalCode(null);
						else{
							composed_of1.getOrderRef().setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							composed_of1.getOrderRef().setShipRegion(null);
						else{
							composed_of1.getOrderRef().setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							composed_of1.getOrderRef().setShippedDate(null);
						else{
							composed_of1.getOrderRef().setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("products");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Composed_of composed_of2 = (Composed_of) composed_of1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Product.productID for field ProductID			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ProductID")) {
								if(nestedRow.getAs("ProductID")==null)
									composed_of2.getProductRef().setProductID(null);
								else{
									composed_of2.getProductRef().setProductID(Util.getIntegerValue(nestedRow.getAs("ProductID")));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.productName for field ProductName			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ProductName")) {
								if(nestedRow.getAs("ProductName")==null)
									composed_of2.getProductRef().setProductName(null);
								else{
									composed_of2.getProductRef().setProductName(Util.getStringValue(nestedRow.getAs("ProductName")));
									toAdd2 = true;					
									}
							}
							// Field 'UnitPrice' is mapped to 'unitPrice' attribute of rel 'composed_of' 
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("UnitPrice")) {
								if(nestedRow.getAs("UnitPrice")==null)
									composed_of2.setUnitPrice(null);
								else{
									composed_of2.setUnitPrice(Util.getDoubleValue(nestedRow.getAs("UnitPrice")));
									toAdd2 = true;					
									}
								}
							// Field 'Quantity' is mapped to 'quantity' attribute of rel 'composed_of' 
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Quantity")) {
								if(nestedRow.getAs("Quantity")==null)
									composed_of2.setQuantity(null);
								else{
									composed_of2.setQuantity(Util.getIntegerValue(nestedRow.getAs("Quantity")));
									toAdd2 = true;					
									}
								}
							// Field 'Discount' is mapped to 'discount' attribute of rel 'composed_of' 
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Discount")) {
								if(nestedRow.getAs("Discount")==null)
									composed_of2.setDiscount(null);
								else{
									composed_of2.setDiscount(Util.getDoubleValue(nestedRow.getAs("Discount")));
									toAdd2 = true;					
									}
								}
							if(toAdd2 && ((orderRef_condition == null || orderRef_refilter.booleanValue() || orderRef_condition.evaluate(composed_of2.getOrderRef()))&&(productRef_condition == null || productRef_refilter.booleanValue() || productRef_condition.evaluate(composed_of2.getProductRef())))) {
								if(!(composed_of2.getOrderRef().equals(new Order())) && !(composed_of2.getProductRef().equals(new Product())))
									list_res.add(composed_of2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						if(!(composed_of1.getOrderRef().equals(new Order())) && !(composed_of1.getProductRef().equals(new Product())))
							list_res.add(composed_of1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Composed_of.class));
			// TODO drop duplicates based on roles ids
			res= res.dropDuplicates(new String[]{"orderRef.orderID","productRef.productID"});
			return res;
	}
	
	public Dataset<Composed_of> getComposed_ofList(
		Condition<OrderAttribute> orderRef_condition,
		Condition<ProductAttribute> productRef_condition,
		Condition<Composed_ofAttribute> composed_of_condition
	){
		Composed_ofServiceImpl composed_ofService = this;
		OrderService orderService = new OrderServiceImpl();  
		ProductService productService = new ProductServiceImpl();
		MutableBoolean orderRef_refilter = new MutableBoolean(false);
		List<Dataset<Composed_of>> datasetsPOJO = new ArrayList<Dataset<Composed_of>>();
		boolean all_already_persisted = false;
		MutableBoolean productRef_refilter = new MutableBoolean(false);
		MutableBoolean composed_of_refilter = new MutableBoolean(false);
		org.apache.spark.sql.Column joinCondition = null;
	
		
		Dataset<Composed_of> res_composed_of_orderRef;
		Dataset<Order> res_Order;
		// Role 'productRef' mapped to EmbeddedObject 'products' 'Order' containing 'Product' 
		productRef_refilter = new MutableBoolean(false);
		res_composed_of_orderRef = composed_ofService.getComposed_ofListInmyMongoDBOrdersproducts(productRef_condition, orderRef_condition, productRef_refilter, orderRef_refilter);
		
		datasetsPOJO.add(res_composed_of_orderRef);
		
		
		//Join datasets or return 
		Dataset<Composed_of> res = fullOuterJoinsComposed_of(datasetsPOJO);
		if(res == null)
			return null;
	
		Dataset<Order> lonelyOrderRef = null;
		Dataset<Product> lonelyProductRef = null;
		
	
		List<Dataset<Product>> lonelyproductRefList = new ArrayList<Dataset<Product>>();
		lonelyproductRefList.add(productService.getProductListInProductFromMyRedis(productRef_condition, new MutableBoolean(false)));
		lonelyProductRef = ProductService.fullOuterJoinsProduct(lonelyproductRefList);
		if(lonelyProductRef != null) {
			res = fullLeftOuterJoinBetweenComposed_ofAndProductRef(res, lonelyProductRef);
		}	
	
		
		if(orderRef_refilter.booleanValue() || productRef_refilter.booleanValue() || composed_of_refilter.booleanValue())
			res = res.filter((FilterFunction<Composed_of>) r -> (orderRef_condition == null || orderRef_condition.evaluate(r.getOrderRef())) && (productRef_condition == null || productRef_condition.evaluate(r.getProductRef())) && (composed_of_condition == null || composed_of_condition.evaluate(r)));
		
	
		return res;
	
	}
	
	public Dataset<Composed_of> getComposed_ofListByOrderRefCondition(
		Condition<OrderAttribute> orderRef_condition
	){
		return getComposed_ofList(orderRef_condition, null, null);
	}
	
	public Dataset<Composed_of> getComposed_ofListByOrderRef(Order orderRef) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.orderID, Operator.EQUALS, orderRef.getOrderID());
		Dataset<Composed_of> res = getComposed_ofListByOrderRefCondition(cond);
	return res;
	}
	public Dataset<Composed_of> getComposed_ofListByProductRefCondition(
		Condition<ProductAttribute> productRef_condition
	){
		return getComposed_ofList(null, productRef_condition, null);
	}
	
	public Dataset<Composed_of> getComposed_ofListByProductRef(Product productRef) {
		Condition<ProductAttribute> cond = null;
		cond = Condition.simple(ProductAttribute.productID, Operator.EQUALS, productRef.getProductID());
		Dataset<Composed_of> res = getComposed_ofListByProductRefCondition(cond);
	return res;
	}
	
	public Dataset<Composed_of> getComposed_ofListByComposed_ofCondition(
		Condition<Composed_ofAttribute> composed_of_condition
	){
		return getComposed_ofList(null, null, composed_of_condition);
	}
	
	public void insertComposed_of(Composed_of composed_of){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		insertComposed_ofInEmbeddedStructOrdersInMyMongoDB(composed_of);
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	public 	boolean insertComposed_ofInEmbeddedStructOrdersInMyMongoDB(Composed_of composed_of){
	 	// Rel 'composed_of' Insert in embedded structure 'Orders'
	
		Order order = composed_of.getOrderRef();
		Product product = composed_of.getProductRef();
		Bson filter= new Document();
		Bson updateOp;
		String addToSet;
		List<String> fieldName= new ArrayList();
		List<Bson> arrayFilterCond = new ArrayList();
		Document docproducts_1 = new Document();
		
		// level 1 ascending
		 // Rel attributes. Only simple ones.  
		docproducts_1.append("UnitPrice",composed_of.getUnitPrice());
		docproducts_1.append("Quantity",composed_of.getQuantity());
		docproducts_1.append("Discount",composed_of.getDiscount());
		filter = eq("ProductID",product.getProductID());
		updateOp = addToSet("products", docproducts_1);
		DBConnectionMgr.update(filter, updateOp, "Orders", "myMongoDB");					
		return true;
	}
	
	
	
	
	public void updateComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		//TODO
	}
	
	public void updateComposed_ofListByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		updateComposed_ofList(orderRef_condition, null, null, set);
	}
	
	public void updateComposed_ofListByOrderRef(pojo.Order orderRef, conditions.SetClause<conditions.Composed_ofAttribute> set) {
		// TODO using id for selecting
		return;
	}
	public void updateComposed_ofListByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		updateComposed_ofList(null, productRef_condition, null, set);
	}
	
	public void updateComposed_ofListByProductRef(pojo.Product productRef, conditions.SetClause<conditions.Composed_ofAttribute> set) {
		// TODO using id for selecting
		return;
	}
	
	public void updateComposed_ofListByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	){
		updateComposed_ofList(null, null, composed_of_condition, set);
	}
	
	public void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition){
			//TODO
		}
	
	public void deleteComposed_ofListByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition
	){
		deleteComposed_ofList(orderRef_condition, null, null);
	}
	
	public void deleteComposed_ofListByOrderRef(pojo.Order orderRef) {
		// TODO using id for selecting
		return;
	}
	public void deleteComposed_ofListByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition
	){
		deleteComposed_ofList(null, productRef_condition, null);
	}
	
	public void deleteComposed_ofListByProductRef(pojo.Product productRef) {
		// TODO using id for selecting
		return;
	}
	
	public void deleteComposed_ofListByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		deleteComposed_ofList(null, null, composed_of_condition);
	}
		
}
