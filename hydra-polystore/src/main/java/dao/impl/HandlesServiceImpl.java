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
import conditions.HandlesAttribute;
import conditions.Operator;
import tdo.*;
import pojo.*;
import tdo.OrderTDO;
import tdo.HandlesTDO;
import conditions.OrderAttribute;
import dao.services.OrderService;
import tdo.EmployeeTDO;
import tdo.HandlesTDO;
import conditions.EmployeeAttribute;
import dao.services.EmployeeService;
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

public class HandlesServiceImpl extends dao.services.HandlesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HandlesServiceImpl.class);
	
	
	// Left side 'EmployeeRef' of reference [orderHandler ]
	public Dataset<OrderTDO> getOrderTDOListOrderInOrderHandlerInOrdersFromMyMongoDB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<OrderTDO> res = dataset.flatMap((FlatMapFunction<Row, OrderTDO>) r -> {
				Set<OrderTDO> list_res = new HashSet<OrderTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrderTDO order1 = new OrderTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.orderID for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID") == null){
							order1.setOrderID(null);
						}else{
							order1.setOrderID(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight") == null){
							order1.setFreight(null);
						}else{
							order1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							order1.setOrderDate(null);
						}else{
							order1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate") == null){
							order1.setRequiredDate(null);
						}else{
							order1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress") == null){
							order1.setShipAddress(null);
						}else{
							order1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity") == null){
							order1.setShipCity(null);
						}else{
							order1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry") == null){
							order1.setShipCountry(null);
						}else{
							order1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName") == null){
							order1.setShipName(null);
						}else{
							order1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode") == null){
							order1.setShipPostalCode(null);
						}else{
							order1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion") == null){
							order1.setShipRegion(null);
						}else{
							order1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate") == null){
							order1.setShippedDate(null);
						}else{
							order1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeRef for reference orderHandler . Reference field : EmployeeRef
					nestedRow =  r1;
					if(nestedRow != null) {
						order1.setMyMongoDB_Orders_orderHandler_source_EmployeeRef(nestedRow.getAs("EmployeeRef") == null ? null : nestedRow.getAs("EmployeeRef").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(order1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(OrderTDO.class));
		res= res.dropDuplicates(new String[]{"orderID"});
		return res;
	}
	
	// Right side 'EmployeeID' of reference [orderHandler ]
	public Dataset<EmployeeTDO> getEmployeeTDOListEmployeeRefInOrderHandlerInOrdersFromMyMongoDB(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeeServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<EmployeeTDO> res = dataset.flatMap((FlatMapFunction<Row, EmployeeTDO>) r -> {
				Set<EmployeeTDO> list_res = new HashSet<EmployeeTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				EmployeeTDO employee1 = new EmployeeTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employee.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID") == null){
							employee1.setEmployeeID(null);
						}else{
							employee1.setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address") == null){
							employee1.setAddress(null);
						}else{
							employee1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate") == null){
							employee1.setBirthDate(null);
						}else{
							employee1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City") == null){
							employee1.setCity(null);
						}else{
							employee1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country") == null){
							employee1.setCountry(null);
						}else{
							employee1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension") == null){
							employee1.setExtension(null);
						}else{
							employee1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName") == null){
							employee1.setFirstName(null);
						}else{
							employee1.setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate") == null){
							employee1.setHireDate(null);
						}else{
							employee1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone") == null){
							employee1.setHomePhone(null);
						}else{
							employee1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName") == null){
							employee1.setLastName(null);
						}else{
							employee1.setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes") == null){
							employee1.setNotes(null);
						}else{
							employee1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo") == null){
							employee1.setPhoto(null);
						}else{
							employee1.setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath") == null){
							employee1.setPhotoPath(null);
						}else{
							employee1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode") == null){
							employee1.setPostalCode(null);
						}else{
							employee1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region") == null){
							employee1.setRegion(null);
						}else{
							employee1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary") == null){
							employee1.setSalary(null);
						}else{
							employee1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title") == null){
							employee1.setTitle(null);
						}else{
							employee1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy") == null){
							employee1.setTitleOfCourtesy(null);
						}else{
							employee1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					
						// field  EmployeeID for reference orderHandler . Reference field : EmployeeID
					nestedRow =  r1;
					if(nestedRow != null) {
						employee1.setMyMongoDB_Orders_orderHandler_target_EmployeeID(nestedRow.getAs("EmployeeID") == null ? null : nestedRow.getAs("EmployeeID").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(employee1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(EmployeeTDO.class));
		res= res.dropDuplicates(new String[]{"employeeID"});
		return res;
	}
	
	
	
	
	public Dataset<Handles> getHandlesList(
		Condition<OrderAttribute> order_condition,
		Condition<EmployeeAttribute> employeeRef_condition){
			HandlesServiceImpl handlesService = this;
			OrderService orderService = new OrderServiceImpl();  
			EmployeeService employeeService = new EmployeeServiceImpl();
			MutableBoolean order_refilter = new MutableBoolean(false);
			List<Dataset<Handles>> datasetsPOJO = new ArrayList<Dataset<Handles>>();
			boolean all_already_persisted = false;
			MutableBoolean employeeRef_refilter = new MutableBoolean(false);
			
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'order' in reference 'orderHandler'. A->B Scenario
			employeeRef_refilter = new MutableBoolean(false);
			Dataset<OrderTDO> orderTDOorderHandlerorder = handlesService.getOrderTDOListOrderInOrderHandlerInOrdersFromMyMongoDB(order_condition, order_refilter);
			Dataset<EmployeeTDO> employeeTDOorderHandleremployeeRef = handlesService.getEmployeeTDOListEmployeeRefInOrderHandlerInOrdersFromMyMongoDB(employeeRef_condition, employeeRef_refilter);
			
			Dataset<Row> res_orderHandler_temp = orderTDOorderHandlerorder.join(employeeTDOorderHandleremployeeRef
					.withColumnRenamed("employeeID", "Employee_employeeID")
					.withColumnRenamed("address", "Employee_address")
					.withColumnRenamed("birthDate", "Employee_birthDate")
					.withColumnRenamed("city", "Employee_city")
					.withColumnRenamed("country", "Employee_country")
					.withColumnRenamed("extension", "Employee_extension")
					.withColumnRenamed("firstName", "Employee_firstName")
					.withColumnRenamed("hireDate", "Employee_hireDate")
					.withColumnRenamed("homePhone", "Employee_homePhone")
					.withColumnRenamed("lastName", "Employee_lastName")
					.withColumnRenamed("notes", "Employee_notes")
					.withColumnRenamed("photo", "Employee_photo")
					.withColumnRenamed("photoPath", "Employee_photoPath")
					.withColumnRenamed("postalCode", "Employee_postalCode")
					.withColumnRenamed("region", "Employee_region")
					.withColumnRenamed("salary", "Employee_salary")
					.withColumnRenamed("title", "Employee_title")
					.withColumnRenamed("titleOfCourtesy", "Employee_titleOfCourtesy")
					.withColumnRenamed("logEvents", "Employee_logEvents"),
					orderTDOorderHandlerorder.col("myMongoDB_Orders_orderHandler_source_EmployeeRef").equalTo(employeeTDOorderHandleremployeeRef.col("myMongoDB_Orders_orderHandler_target_EmployeeID")));
		
			Dataset<Handles> res_orderHandler = res_orderHandler_temp.map(
				(MapFunction<Row, Handles>) r -> {
					Handles res = new Handles();
					Order A = new Order();
					Employee B = new Employee();
					A.setOrderID(Util.getIntegerValue(r.getAs("orderID")));
					A.setFreight(Util.getDoubleValue(r.getAs("freight")));
					A.setOrderDate(Util.getLocalDateValue(r.getAs("orderDate")));
					A.setRequiredDate(Util.getLocalDateValue(r.getAs("requiredDate")));
					A.setShipAddress(Util.getStringValue(r.getAs("shipAddress")));
					A.setShipCity(Util.getStringValue(r.getAs("shipCity")));
					A.setShipCountry(Util.getStringValue(r.getAs("shipCountry")));
					A.setShipName(Util.getStringValue(r.getAs("shipName")));
					A.setShipPostalCode(Util.getStringValue(r.getAs("shipPostalCode")));
					A.setShipRegion(Util.getStringValue(r.getAs("shipRegion")));
					A.setShippedDate(Util.getLocalDateValue(r.getAs("shippedDate")));
					A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
					B.setEmployeeID(Util.getIntegerValue(r.getAs("Employee_employeeID")));
					B.setAddress(Util.getStringValue(r.getAs("Employee_address")));
					B.setBirthDate(Util.getLocalDateValue(r.getAs("Employee_birthDate")));
					B.setCity(Util.getStringValue(r.getAs("Employee_city")));
					B.setCountry(Util.getStringValue(r.getAs("Employee_country")));
					B.setExtension(Util.getStringValue(r.getAs("Employee_extension")));
					B.setFirstName(Util.getStringValue(r.getAs("Employee_firstName")));
					B.setHireDate(Util.getLocalDateValue(r.getAs("Employee_hireDate")));
					B.setHomePhone(Util.getStringValue(r.getAs("Employee_homePhone")));
					B.setLastName(Util.getStringValue(r.getAs("Employee_lastName")));
					B.setNotes(Util.getStringValue(r.getAs("Employee_notes")));
					B.setPhoto(Util.getByteArrayValue(r.getAs("Employee_photo")));
					B.setPhotoPath(Util.getStringValue(r.getAs("Employee_photoPath")));
					B.setPostalCode(Util.getStringValue(r.getAs("Employee_postalCode")));
					B.setRegion(Util.getStringValue(r.getAs("Employee_region")));
					B.setSalary(Util.getDoubleValue(r.getAs("Employee_salary")));
					B.setTitle(Util.getStringValue(r.getAs("Employee_title")));
					B.setTitleOfCourtesy(Util.getStringValue(r.getAs("Employee_titleOfCourtesy")));
					B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Employee_logEvents")));
						
					res.setOrder(A);
					res.setEmployeeRef(B);
					return res;
				},Encoders.bean(Handles.class)
			);
		
			datasetsPOJO.add(res_orderHandler);
		
			
			Dataset<Handles> res_handles_order;
			Dataset<Order> res_Order;
			
			
			//Join datasets or return 
			Dataset<Handles> res = fullOuterJoinsHandles(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyOrder = null;
			Dataset<Employee> lonelyEmployeeRef = null;
			
		
		
			
			if(order_refilter.booleanValue() || employeeRef_refilter.booleanValue())
				res = res.filter((FilterFunction<Handles>) r -> (order_condition == null || order_condition.evaluate(r.getOrder())) && (employeeRef_condition == null || employeeRef_condition.evaluate(r.getEmployeeRef())));
			
		
			return res;
		
		}
	
	public Dataset<Handles> getHandlesListByOrderCondition(
		Condition<OrderAttribute> order_condition
	){
		return getHandlesList(order_condition, null);
	}
	
	public Handles getHandlesByOrder(Order order) {
		Condition<OrderAttribute> cond = null;
		cond = Condition.simple(OrderAttribute.orderID, Operator.EQUALS, order.getOrderID());
		Dataset<Handles> res = getHandlesListByOrderCondition(cond);
		List<Handles> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<Handles> getHandlesListByEmployeeRefCondition(
		Condition<EmployeeAttribute> employeeRef_condition
	){
		return getHandlesList(null, employeeRef_condition);
	}
	
	public Dataset<Handles> getHandlesListByEmployeeRef(Employee employeeRef) {
		Condition<EmployeeAttribute> cond = null;
		cond = Condition.simple(EmployeeAttribute.employeeID, Operator.EQUALS, employeeRef.getEmployeeID());
		Dataset<Handles> res = getHandlesListByEmployeeRefCondition(cond);
	return res;
	}
	
	
	
	public void deleteHandlesList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition){
			//TODO
		}
	
	public void deleteHandlesListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteHandlesList(order_condition, null);
	}
	
	public void deleteHandlesByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteHandlesListByEmployeeRefCondition(
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition
	){
		deleteHandlesList(null, employeeRef_condition);
	}
	
	public void deleteHandlesListByEmployeeRef(pojo.Employee employeeRef) {
		// TODO using id for selecting
		return;
	}
		
}
