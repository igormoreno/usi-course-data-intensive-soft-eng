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
import conditions.BuysAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrderTDO;
import tdo.BuysTDO;
import conditions.OrderAttribute;
import dao.services.OrderService;
import tdo.CustomerTDO;
import tdo.BuysTDO;
import conditions.CustomerAttribute;
import dao.services.CustomerService;
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

public class BuysServiceImpl extends dao.services.BuysService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuysServiceImpl.class);
	
	// method accessing the embedded object customer mapped to role boughtOrder
	public Dataset<Buys> getBuysListInmyMongoDBOrderscustomer(Condition<OrderAttribute> boughtOrder_condition, Condition<CustomerAttribute> customerRef_condition, MutableBoolean boughtOrder_refilter, MutableBoolean customerRef_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(boughtOrder_condition ,boughtOrder_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = CustomerServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(customerRef_condition ,customerRef_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
		
			Dataset<Buys> res = dataset.flatMap((FlatMapFunction<Row, Buys>) r -> {
					List<Buys> list_res = new ArrayList<Buys>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Buys buys1 = new Buys();
					buys1.setBoughtOrder(new Order());
					buys1.setCustomerRef(new Customer());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.orderID for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							buys1.getBoughtOrder().setOrderID(null);
						else{
							buys1.getBoughtOrder().setOrderID(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							buys1.getBoughtOrder().setFreight(null);
						else{
							buys1.getBoughtOrder().setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							buys1.getBoughtOrder().setOrderDate(null);
						else{
							buys1.getBoughtOrder().setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							buys1.getBoughtOrder().setRequiredDate(null);
						else{
							buys1.getBoughtOrder().setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							buys1.getBoughtOrder().setShipAddress(null);
						else{
							buys1.getBoughtOrder().setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							buys1.getBoughtOrder().setShipCity(null);
						else{
							buys1.getBoughtOrder().setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							buys1.getBoughtOrder().setShipCountry(null);
						else{
							buys1.getBoughtOrder().setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							buys1.getBoughtOrder().setShipName(null);
						else{
							buys1.getBoughtOrder().setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							buys1.getBoughtOrder().setShipPostalCode(null);
						else{
							buys1.getBoughtOrder().setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							buys1.getBoughtOrder().setShipRegion(null);
						else{
							buys1.getBoughtOrder().setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							buys1.getBoughtOrder().setShippedDate(null);
						else{
							buys1.getBoughtOrder().setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.iD for field CustomerID			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("CustomerID")) {
						if(nestedRow.getAs("CustomerID")==null)
							buys1.getCustomerRef().setID(null);
						else{
							buys1.getCustomerRef().setID(Util.getStringValue(nestedRow.getAs("CustomerID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.contactName for field ContactName			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("customer");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ContactName")) {
						if(nestedRow.getAs("ContactName")==null)
							buys1.getCustomerRef().setContactName(null);
						else{
							buys1.getCustomerRef().setContactName(Util.getStringValue(nestedRow.getAs("ContactName")));
							toAdd1 = true;					
							}
					}
					if(toAdd1 ) {
						if(!(buys1.getBoughtOrder().equals(new Order())) && !(buys1.getCustomerRef().equals(new Customer())))
							list_res.add(buys1);
						addedInList = true;
					} 
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Buys.class));
			// TODO drop duplicates based on roles ids
			res= res.dropDuplicates(new String[]{"boughtOrder.orderID","customerRef.iD"});
			return res;
	}
	
	
	public Dataset<Buys> getBuysList(
		Condition<OrderAttribute> boughtOrder_condition,
		Condition<CustomerAttribute> customerRef_condition){
			BuysServiceImpl buysService = this;
			OrderService orderService = new OrderServiceImpl();  
			CustomerService customerService = new CustomerServiceImpl();
			MutableBoolean boughtOrder_refilter = new MutableBoolean(false);
			List<Dataset<Buys>> datasetsPOJO = new ArrayList<Dataset<Buys>>();
			boolean all_already_persisted = false;
			MutableBoolean customerRef_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
		
			
			Dataset<Buys> res_buys_boughtOrder;
			Dataset<Order> res_Order;
			// Role 'boughtOrder' mapped to EmbeddedObject 'customer' - 'Customer' containing 'Order'
			customerRef_refilter = new MutableBoolean(false);
			res_buys_boughtOrder = buysService.getBuysListInmyMongoDBOrderscustomer(boughtOrder_condition, customerRef_condition, boughtOrder_refilter, customerRef_refilter);
		 	
			datasetsPOJO.add(res_buys_boughtOrder);
			
			
			//Join datasets or return 
			Dataset<Buys> res = fullOuterJoinsBuys(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyBoughtOrder = null;
			Dataset<Customer> lonelyCustomerRef = null;
			
		
			List<Dataset<Customer>> lonelycustomerRefList = new ArrayList<Dataset<Customer>>();
			lonelycustomerRefList.add(customerService.getCustomerListInCustomersFromMyMongoDB(customerRef_condition, new MutableBoolean(false)));
			lonelyCustomerRef = CustomerService.fullOuterJoinsCustomer(lonelycustomerRefList);
			if(lonelyCustomerRef != null) {
				res = fullLeftOuterJoinBetweenBuysAndCustomerRef(res, lonelyCustomerRef);
			}	
		
			
			if(boughtOrder_refilter.booleanValue() || customerRef_refilter.booleanValue())
				res = res.filter((FilterFunction<Buys>) r -> (boughtOrder_condition == null || boughtOrder_condition.evaluate(r.getBoughtOrder())) && (customerRef_condition == null || customerRef_condition.evaluate(r.getCustomerRef())));
			
		
			return res;
		
		}
	
	public Dataset<Buys> getBuysListByBoughtOrderCondition(
		Condition<OrderAttribute> boughtOrder_condition
	){
		return getBuysList(boughtOrder_condition, null);
	}
	
	public Buys getBuysByBoughtOrder(Order boughtOrder) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.orderID, Operator.EQUALS, boughtOrder.getOrderID());
		Dataset<Buys> res = getBuysListByBoughtOrderCondition(cond);
		List<Buys> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Buys> getBuysListByCustomerRefCondition(
		Condition<CustomerAttribute> customerRef_condition
	){
		return getBuysList(null, customerRef_condition);
	}
	
	public Dataset<Buys> getBuysListByCustomerRef(Customer customerRef) {
		Condition<CustomerAttribute> cond = null;
		cond = Condition.simple(CustomerAttribute.iD, Operator.EQUALS, customerRef.getID());
		Dataset<Buys> res = getBuysListByCustomerRefCondition(cond);
	return res;
	}
	
	
	
	public void deleteBuysList(
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition){
			//TODO
		}
	
	public void deleteBuysListByBoughtOrderCondition(
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition
	){
		deleteBuysList(boughtOrder_condition, null);
	}
	
	public void deleteBuysByBoughtOrder(pojo.Order boughtOrder) {
		// TODO using id for selecting
		return;
	}
	public void deleteBuysListByCustomerRefCondition(
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition
	){
		deleteBuysList(null, customerRef_condition);
	}
	
	public void deleteBuysListByCustomerRef(pojo.Customer customerRef) {
		// TODO using id for selecting
		return;
	}
		
}
