package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Handles;
import java.time.LocalDate;
import java.time.LocalDateTime;
import tdo.*;
import pojo.*;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class HandlesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HandlesService.class);
	
	
	// Left side 'EmployeeRef' of reference [orderHandler ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInOrderHandlerInOrdersFromMyMongoDB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'EmployeeID' of reference [orderHandler ]
	public abstract Dataset<EmployeeTDO> getEmployeeTDOListEmployeeRefInOrderHandlerInOrdersFromMyMongoDB(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public static Dataset<Handles> fullLeftOuterJoinBetweenHandlesAndOrder(Dataset<Handles> d1, Dataset<Order> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("orderID", "A_orderID")
			.withColumnRenamed("freight", "A_freight")
			.withColumnRenamed("orderDate", "A_orderDate")
			.withColumnRenamed("requiredDate", "A_requiredDate")
			.withColumnRenamed("shipAddress", "A_shipAddress")
			.withColumnRenamed("shipCity", "A_shipCity")
			.withColumnRenamed("shipCountry", "A_shipCountry")
			.withColumnRenamed("shipName", "A_shipName")
			.withColumnRenamed("shipPostalCode", "A_shipPostalCode")
			.withColumnRenamed("shipRegion", "A_shipRegion")
			.withColumnRenamed("shippedDate", "A_shippedDate")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("order.orderID").equalTo(d2_.col("A_orderID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Handles>) r -> {
				Handles res = new Handles();
	
				Order order = new Order();
				Object o = r.getAs("order");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setOrderID(Util.getIntegerValue(r2.getAs("orderID")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				Integer orderID = Util.getIntegerValue(r.getAs("A_orderID"));
				if (order.getOrderID() != null && orderID != null && !order.getOrderID().equals(orderID)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.orderID': " + order.getOrderID() + " and " + orderID + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.orderID': " + order.getOrderID() + " and " + orderID + "." );
				}
				if(orderID != null)
					order.setOrderID(orderID);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (order.getFreight() != null && freight != null && !order.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.freight': " + order.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.freight': " + order.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					order.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (order.getOrderDate() != null && orderDate != null && !order.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.orderDate': " + order.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					order.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (order.getRequiredDate() != null && requiredDate != null && !order.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.requiredDate': " + order.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					order.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (order.getShipAddress() != null && shipAddress != null && !order.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shipAddress': " + order.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					order.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (order.getShipCity() != null && shipCity != null && !order.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shipCity': " + order.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					order.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (order.getShipCountry() != null && shipCountry != null && !order.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shipCountry': " + order.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					order.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (order.getShipName() != null && shipName != null && !order.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shipName': " + order.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shipName': " + order.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					order.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (order.getShipPostalCode() != null && shipPostalCode != null && !order.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shipPostalCode': " + order.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					order.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (order.getShipRegion() != null && shipRegion != null && !order.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shipRegion': " + order.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					order.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (order.getShippedDate() != null && shippedDate != null && !order.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.shippedDate': " + order.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					order.setShippedDate(shippedDate);
	
				o = r.getAs("employeeRef");
				Employee employeeRef = new Employee();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employeeRef.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						employeeRef.setAddress(Util.getStringValue(r2.getAs("address")));
						employeeRef.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employeeRef.setCity(Util.getStringValue(r2.getAs("city")));
						employeeRef.setCountry(Util.getStringValue(r2.getAs("country")));
						employeeRef.setExtension(Util.getStringValue(r2.getAs("extension")));
						employeeRef.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						employeeRef.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employeeRef.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employeeRef.setLastName(Util.getStringValue(r2.getAs("lastName")));
						employeeRef.setNotes(Util.getStringValue(r2.getAs("notes")));
						employeeRef.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						employeeRef.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employeeRef.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employeeRef.setRegion(Util.getStringValue(r2.getAs("region")));
						employeeRef.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						employeeRef.setTitle(Util.getStringValue(r2.getAs("title")));
						employeeRef.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						employeeRef = (Employee) o;
					}
				}
	
				res.setEmployeeRef(employeeRef);
	
				return res;
		}, Encoders.bean(Handles.class));
	
		
		
	}
	public static Dataset<Handles> fullLeftOuterJoinBetweenHandlesAndEmployeeRef(Dataset<Handles> d1, Dataset<Employee> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("employeeID", "A_employeeID")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("birthDate", "A_birthDate")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("extension", "A_extension")
			.withColumnRenamed("firstName", "A_firstName")
			.withColumnRenamed("hireDate", "A_hireDate")
			.withColumnRenamed("homePhone", "A_homePhone")
			.withColumnRenamed("lastName", "A_lastName")
			.withColumnRenamed("notes", "A_notes")
			.withColumnRenamed("photo", "A_photo")
			.withColumnRenamed("photoPath", "A_photoPath")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("salary", "A_salary")
			.withColumnRenamed("title", "A_title")
			.withColumnRenamed("titleOfCourtesy", "A_titleOfCourtesy")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("employeeRef.employeeID").equalTo(d2_.col("A_employeeID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Handles>) r -> {
				Handles res = new Handles();
	
				Employee employeeRef = new Employee();
				Object o = r.getAs("employeeRef");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						employeeRef.setEmployeeID(Util.getIntegerValue(r2.getAs("employeeID")));
						employeeRef.setAddress(Util.getStringValue(r2.getAs("address")));
						employeeRef.setBirthDate(Util.getLocalDateValue(r2.getAs("birthDate")));
						employeeRef.setCity(Util.getStringValue(r2.getAs("city")));
						employeeRef.setCountry(Util.getStringValue(r2.getAs("country")));
						employeeRef.setExtension(Util.getStringValue(r2.getAs("extension")));
						employeeRef.setFirstName(Util.getStringValue(r2.getAs("firstName")));
						employeeRef.setHireDate(Util.getLocalDateValue(r2.getAs("hireDate")));
						employeeRef.setHomePhone(Util.getStringValue(r2.getAs("homePhone")));
						employeeRef.setLastName(Util.getStringValue(r2.getAs("lastName")));
						employeeRef.setNotes(Util.getStringValue(r2.getAs("notes")));
						employeeRef.setPhoto(Util.getByteArrayValue(r2.getAs("photo")));
						employeeRef.setPhotoPath(Util.getStringValue(r2.getAs("photoPath")));
						employeeRef.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						employeeRef.setRegion(Util.getStringValue(r2.getAs("region")));
						employeeRef.setSalary(Util.getDoubleValue(r2.getAs("salary")));
						employeeRef.setTitle(Util.getStringValue(r2.getAs("title")));
						employeeRef.setTitleOfCourtesy(Util.getStringValue(r2.getAs("titleOfCourtesy")));
					} 
					if(o instanceof Employee) {
						employeeRef = (Employee) o;
					}
				}
	
				res.setEmployeeRef(employeeRef);
	
				Integer employeeID = Util.getIntegerValue(r.getAs("A_employeeID"));
				if (employeeRef.getEmployeeID() != null && employeeID != null && !employeeRef.getEmployeeID().equals(employeeID)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.employeeID': " + employeeRef.getEmployeeID() + " and " + employeeID + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.employeeID': " + employeeRef.getEmployeeID() + " and " + employeeID + "." );
				}
				if(employeeID != null)
					employeeRef.setEmployeeID(employeeID);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (employeeRef.getAddress() != null && address != null && !employeeRef.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.address': " + employeeRef.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.address': " + employeeRef.getAddress() + " and " + address + "." );
				}
				if(address != null)
					employeeRef.setAddress(address);
				LocalDate birthDate = Util.getLocalDateValue(r.getAs("A_birthDate"));
				if (employeeRef.getBirthDate() != null && birthDate != null && !employeeRef.getBirthDate().equals(birthDate)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.birthDate': " + employeeRef.getBirthDate() + " and " + birthDate + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.birthDate': " + employeeRef.getBirthDate() + " and " + birthDate + "." );
				}
				if(birthDate != null)
					employeeRef.setBirthDate(birthDate);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (employeeRef.getCity() != null && city != null && !employeeRef.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.city': " + employeeRef.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.city': " + employeeRef.getCity() + " and " + city + "." );
				}
				if(city != null)
					employeeRef.setCity(city);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (employeeRef.getCountry() != null && country != null && !employeeRef.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.country': " + employeeRef.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.country': " + employeeRef.getCountry() + " and " + country + "." );
				}
				if(country != null)
					employeeRef.setCountry(country);
				String extension = Util.getStringValue(r.getAs("A_extension"));
				if (employeeRef.getExtension() != null && extension != null && !employeeRef.getExtension().equals(extension)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.extension': " + employeeRef.getExtension() + " and " + extension + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.extension': " + employeeRef.getExtension() + " and " + extension + "." );
				}
				if(extension != null)
					employeeRef.setExtension(extension);
				String firstName = Util.getStringValue(r.getAs("A_firstName"));
				if (employeeRef.getFirstName() != null && firstName != null && !employeeRef.getFirstName().equals(firstName)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.firstName': " + employeeRef.getFirstName() + " and " + firstName + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.firstName': " + employeeRef.getFirstName() + " and " + firstName + "." );
				}
				if(firstName != null)
					employeeRef.setFirstName(firstName);
				LocalDate hireDate = Util.getLocalDateValue(r.getAs("A_hireDate"));
				if (employeeRef.getHireDate() != null && hireDate != null && !employeeRef.getHireDate().equals(hireDate)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.hireDate': " + employeeRef.getHireDate() + " and " + hireDate + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.hireDate': " + employeeRef.getHireDate() + " and " + hireDate + "." );
				}
				if(hireDate != null)
					employeeRef.setHireDate(hireDate);
				String homePhone = Util.getStringValue(r.getAs("A_homePhone"));
				if (employeeRef.getHomePhone() != null && homePhone != null && !employeeRef.getHomePhone().equals(homePhone)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.homePhone': " + employeeRef.getHomePhone() + " and " + homePhone + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.homePhone': " + employeeRef.getHomePhone() + " and " + homePhone + "." );
				}
				if(homePhone != null)
					employeeRef.setHomePhone(homePhone);
				String lastName = Util.getStringValue(r.getAs("A_lastName"));
				if (employeeRef.getLastName() != null && lastName != null && !employeeRef.getLastName().equals(lastName)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.lastName': " + employeeRef.getLastName() + " and " + lastName + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.lastName': " + employeeRef.getLastName() + " and " + lastName + "." );
				}
				if(lastName != null)
					employeeRef.setLastName(lastName);
				String notes = Util.getStringValue(r.getAs("A_notes"));
				if (employeeRef.getNotes() != null && notes != null && !employeeRef.getNotes().equals(notes)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.notes': " + employeeRef.getNotes() + " and " + notes + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.notes': " + employeeRef.getNotes() + " and " + notes + "." );
				}
				if(notes != null)
					employeeRef.setNotes(notes);
				byte[] photo = Util.getByteArrayValue(r.getAs("A_photo"));
				if (employeeRef.getPhoto() != null && photo != null && !employeeRef.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.photo': " + employeeRef.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.photo': " + employeeRef.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					employeeRef.setPhoto(photo);
				String photoPath = Util.getStringValue(r.getAs("A_photoPath"));
				if (employeeRef.getPhotoPath() != null && photoPath != null && !employeeRef.getPhotoPath().equals(photoPath)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.photoPath': " + employeeRef.getPhotoPath() + " and " + photoPath + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.photoPath': " + employeeRef.getPhotoPath() + " and " + photoPath + "." );
				}
				if(photoPath != null)
					employeeRef.setPhotoPath(photoPath);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (employeeRef.getPostalCode() != null && postalCode != null && !employeeRef.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.postalCode': " + employeeRef.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.postalCode': " + employeeRef.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					employeeRef.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (employeeRef.getRegion() != null && region != null && !employeeRef.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.region': " + employeeRef.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.region': " + employeeRef.getRegion() + " and " + region + "." );
				}
				if(region != null)
					employeeRef.setRegion(region);
				Double salary = Util.getDoubleValue(r.getAs("A_salary"));
				if (employeeRef.getSalary() != null && salary != null && !employeeRef.getSalary().equals(salary)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.salary': " + employeeRef.getSalary() + " and " + salary + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.salary': " + employeeRef.getSalary() + " and " + salary + "." );
				}
				if(salary != null)
					employeeRef.setSalary(salary);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (employeeRef.getTitle() != null && title != null && !employeeRef.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.title': " + employeeRef.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.title': " + employeeRef.getTitle() + " and " + title + "." );
				}
				if(title != null)
					employeeRef.setTitle(title);
				String titleOfCourtesy = Util.getStringValue(r.getAs("A_titleOfCourtesy"));
				if (employeeRef.getTitleOfCourtesy() != null && titleOfCourtesy != null && !employeeRef.getTitleOfCourtesy().equals(titleOfCourtesy)) {
					res.addLogEvent("Data consistency problem for [Handles - different values found for attribute 'Handles.titleOfCourtesy': " + employeeRef.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
					logger.warn("Data consistency problem for [Handles - different values found for attribute 'Handles.titleOfCourtesy': " + employeeRef.getTitleOfCourtesy() + " and " + titleOfCourtesy + "." );
				}
				if(titleOfCourtesy != null)
					employeeRef.setTitleOfCourtesy(titleOfCourtesy);
	
				o = r.getAs("order");
				Order order = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setOrderID(Util.getIntegerValue(r2.getAs("orderID")));
						order.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						order.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						order.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						order.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						order.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						order.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						order.setShipName(Util.getStringValue(r2.getAs("shipName")));
						order.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						order.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						order.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				return res;
		}, Encoders.bean(Handles.class));
	
		
		
	}
	
	public static Dataset<Handles> fullOuterJoinsHandles(List<Dataset<Handles>> datasetsPOJO) {
		return fullOuterJoinsHandles(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Handles> fullLeftOuterJoinsHandles(List<Dataset<Handles>> datasetsPOJO) {
		return fullOuterJoinsHandles(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Handles> fullOuterJoinsHandles(List<Dataset<Handles>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("order.orderID");
	
		idFields.add("employeeRef.employeeID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Handles> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("order_orderID_" + i, d.col("order.orderID"))
				.withColumn("employeeRef_employeeID_" + i, d.col("employeeRef.employeeID"))
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("employeeRef", "employeeRef_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("order_orderID_0").equalTo(rows.get(1).col("order_orderID_1"));
		joinCond = joinCond.and(rows.get(0).col("employeeRef_employeeID_0").equalTo(rows.get(1).col("employeeRef_employeeID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("order_orderID_" + (i - 1)).equalTo(rows.get(i).col("order_orderID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("employeeRef_employeeID_" + (i - 1)).equalTo(rows.get(i).col("employeeRef_employeeID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Handles>) r -> {
				Handles handles_res = new Handles();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							handles_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							handles_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order order_res = new Order();
					Employee employeeRef_res = new Employee();
					
					// attribute 'Order.orderID'
					Integer firstNotNull_order_orderID = Util.getIntegerValue(r.getAs("order_0.orderID"));
					order_res.setOrderID(firstNotNull_order_orderID);
					// attribute 'Order.freight'
					Double firstNotNull_order_freight = Util.getDoubleValue(r.getAs("order_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double order_freight2 = Util.getDoubleValue(r.getAs("order_" + i + ".freight"));
						if (firstNotNull_order_freight != null && order_freight2 != null && !firstNotNull_order_freight.equals(order_freight2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.freight': " + firstNotNull_order_freight + " and " + order_freight2 + "." );
						}
						if (firstNotNull_order_freight == null && order_freight2 != null) {
							firstNotNull_order_freight = order_freight2;
						}
					}
					order_res.setFreight(firstNotNull_order_freight);
					// attribute 'Order.orderDate'
					LocalDate firstNotNull_order_orderDate = Util.getLocalDateValue(r.getAs("order_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_orderDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".orderDate"));
						if (firstNotNull_order_orderDate != null && order_orderDate2 != null && !firstNotNull_order_orderDate.equals(order_orderDate2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_order_orderDate + " and " + order_orderDate2 + "." );
						}
						if (firstNotNull_order_orderDate == null && order_orderDate2 != null) {
							firstNotNull_order_orderDate = order_orderDate2;
						}
					}
					order_res.setOrderDate(firstNotNull_order_orderDate);
					// attribute 'Order.requiredDate'
					LocalDate firstNotNull_order_requiredDate = Util.getLocalDateValue(r.getAs("order_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_requiredDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".requiredDate"));
						if (firstNotNull_order_requiredDate != null && order_requiredDate2 != null && !firstNotNull_order_requiredDate.equals(order_requiredDate2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_order_requiredDate + " and " + order_requiredDate2 + "." );
						}
						if (firstNotNull_order_requiredDate == null && order_requiredDate2 != null) {
							firstNotNull_order_requiredDate = order_requiredDate2;
						}
					}
					order_res.setRequiredDate(firstNotNull_order_requiredDate);
					// attribute 'Order.shipAddress'
					String firstNotNull_order_shipAddress = Util.getStringValue(r.getAs("order_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipAddress2 = Util.getStringValue(r.getAs("order_" + i + ".shipAddress"));
						if (firstNotNull_order_shipAddress != null && order_shipAddress2 != null && !firstNotNull_order_shipAddress.equals(order_shipAddress2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_order_shipAddress + " and " + order_shipAddress2 + "." );
						}
						if (firstNotNull_order_shipAddress == null && order_shipAddress2 != null) {
							firstNotNull_order_shipAddress = order_shipAddress2;
						}
					}
					order_res.setShipAddress(firstNotNull_order_shipAddress);
					// attribute 'Order.shipCity'
					String firstNotNull_order_shipCity = Util.getStringValue(r.getAs("order_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCity2 = Util.getStringValue(r.getAs("order_" + i + ".shipCity"));
						if (firstNotNull_order_shipCity != null && order_shipCity2 != null && !firstNotNull_order_shipCity.equals(order_shipCity2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_order_shipCity + " and " + order_shipCity2 + "." );
						}
						if (firstNotNull_order_shipCity == null && order_shipCity2 != null) {
							firstNotNull_order_shipCity = order_shipCity2;
						}
					}
					order_res.setShipCity(firstNotNull_order_shipCity);
					// attribute 'Order.shipCountry'
					String firstNotNull_order_shipCountry = Util.getStringValue(r.getAs("order_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipCountry2 = Util.getStringValue(r.getAs("order_" + i + ".shipCountry"));
						if (firstNotNull_order_shipCountry != null && order_shipCountry2 != null && !firstNotNull_order_shipCountry.equals(order_shipCountry2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_order_shipCountry + " and " + order_shipCountry2 + "." );
						}
						if (firstNotNull_order_shipCountry == null && order_shipCountry2 != null) {
							firstNotNull_order_shipCountry = order_shipCountry2;
						}
					}
					order_res.setShipCountry(firstNotNull_order_shipCountry);
					// attribute 'Order.shipName'
					String firstNotNull_order_shipName = Util.getStringValue(r.getAs("order_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipName2 = Util.getStringValue(r.getAs("order_" + i + ".shipName"));
						if (firstNotNull_order_shipName != null && order_shipName2 != null && !firstNotNull_order_shipName.equals(order_shipName2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_order_shipName + " and " + order_shipName2 + "." );
						}
						if (firstNotNull_order_shipName == null && order_shipName2 != null) {
							firstNotNull_order_shipName = order_shipName2;
						}
					}
					order_res.setShipName(firstNotNull_order_shipName);
					// attribute 'Order.shipPostalCode'
					String firstNotNull_order_shipPostalCode = Util.getStringValue(r.getAs("order_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipPostalCode2 = Util.getStringValue(r.getAs("order_" + i + ".shipPostalCode"));
						if (firstNotNull_order_shipPostalCode != null && order_shipPostalCode2 != null && !firstNotNull_order_shipPostalCode.equals(order_shipPostalCode2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_order_shipPostalCode + " and " + order_shipPostalCode2 + "." );
						}
						if (firstNotNull_order_shipPostalCode == null && order_shipPostalCode2 != null) {
							firstNotNull_order_shipPostalCode = order_shipPostalCode2;
						}
					}
					order_res.setShipPostalCode(firstNotNull_order_shipPostalCode);
					// attribute 'Order.shipRegion'
					String firstNotNull_order_shipRegion = Util.getStringValue(r.getAs("order_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String order_shipRegion2 = Util.getStringValue(r.getAs("order_" + i + ".shipRegion"));
						if (firstNotNull_order_shipRegion != null && order_shipRegion2 != null && !firstNotNull_order_shipRegion.equals(order_shipRegion2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_order_shipRegion + " and " + order_shipRegion2 + "." );
						}
						if (firstNotNull_order_shipRegion == null && order_shipRegion2 != null) {
							firstNotNull_order_shipRegion = order_shipRegion2;
						}
					}
					order_res.setShipRegion(firstNotNull_order_shipRegion);
					// attribute 'Order.shippedDate'
					LocalDate firstNotNull_order_shippedDate = Util.getLocalDateValue(r.getAs("order_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate order_shippedDate2 = Util.getLocalDateValue(r.getAs("order_" + i + ".shippedDate"));
						if (firstNotNull_order_shippedDate != null && order_shippedDate2 != null && !firstNotNull_order_shippedDate.equals(order_shippedDate2)) {
							handles_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getOrderID()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_order_shippedDate + " and " + order_shippedDate2 + "." );
						}
						if (firstNotNull_order_shippedDate == null && order_shippedDate2 != null) {
							firstNotNull_order_shippedDate = order_shippedDate2;
						}
					}
					order_res.setShippedDate(firstNotNull_order_shippedDate);
					// attribute 'Employee.employeeID'
					Integer firstNotNull_employeeRef_employeeID = Util.getIntegerValue(r.getAs("employeeRef_0.employeeID"));
					employeeRef_res.setEmployeeID(firstNotNull_employeeRef_employeeID);
					// attribute 'Employee.address'
					String firstNotNull_employeeRef_address = Util.getStringValue(r.getAs("employeeRef_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_address2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".address"));
						if (firstNotNull_employeeRef_address != null && employeeRef_address2 != null && !firstNotNull_employeeRef_address.equals(employeeRef_address2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.address': " + firstNotNull_employeeRef_address + " and " + employeeRef_address2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.address': " + firstNotNull_employeeRef_address + " and " + employeeRef_address2 + "." );
						}
						if (firstNotNull_employeeRef_address == null && employeeRef_address2 != null) {
							firstNotNull_employeeRef_address = employeeRef_address2;
						}
					}
					employeeRef_res.setAddress(firstNotNull_employeeRef_address);
					// attribute 'Employee.birthDate'
					LocalDate firstNotNull_employeeRef_birthDate = Util.getLocalDateValue(r.getAs("employeeRef_0.birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employeeRef_birthDate2 = Util.getLocalDateValue(r.getAs("employeeRef_" + i + ".birthDate"));
						if (firstNotNull_employeeRef_birthDate != null && employeeRef_birthDate2 != null && !firstNotNull_employeeRef_birthDate.equals(employeeRef_birthDate2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_employeeRef_birthDate + " and " + employeeRef_birthDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_employeeRef_birthDate + " and " + employeeRef_birthDate2 + "." );
						}
						if (firstNotNull_employeeRef_birthDate == null && employeeRef_birthDate2 != null) {
							firstNotNull_employeeRef_birthDate = employeeRef_birthDate2;
						}
					}
					employeeRef_res.setBirthDate(firstNotNull_employeeRef_birthDate);
					// attribute 'Employee.city'
					String firstNotNull_employeeRef_city = Util.getStringValue(r.getAs("employeeRef_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_city2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".city"));
						if (firstNotNull_employeeRef_city != null && employeeRef_city2 != null && !firstNotNull_employeeRef_city.equals(employeeRef_city2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.city': " + firstNotNull_employeeRef_city + " and " + employeeRef_city2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.city': " + firstNotNull_employeeRef_city + " and " + employeeRef_city2 + "." );
						}
						if (firstNotNull_employeeRef_city == null && employeeRef_city2 != null) {
							firstNotNull_employeeRef_city = employeeRef_city2;
						}
					}
					employeeRef_res.setCity(firstNotNull_employeeRef_city);
					// attribute 'Employee.country'
					String firstNotNull_employeeRef_country = Util.getStringValue(r.getAs("employeeRef_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_country2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".country"));
						if (firstNotNull_employeeRef_country != null && employeeRef_country2 != null && !firstNotNull_employeeRef_country.equals(employeeRef_country2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.country': " + firstNotNull_employeeRef_country + " and " + employeeRef_country2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.country': " + firstNotNull_employeeRef_country + " and " + employeeRef_country2 + "." );
						}
						if (firstNotNull_employeeRef_country == null && employeeRef_country2 != null) {
							firstNotNull_employeeRef_country = employeeRef_country2;
						}
					}
					employeeRef_res.setCountry(firstNotNull_employeeRef_country);
					// attribute 'Employee.extension'
					String firstNotNull_employeeRef_extension = Util.getStringValue(r.getAs("employeeRef_0.extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_extension2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".extension"));
						if (firstNotNull_employeeRef_extension != null && employeeRef_extension2 != null && !firstNotNull_employeeRef_extension.equals(employeeRef_extension2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_employeeRef_extension + " and " + employeeRef_extension2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_employeeRef_extension + " and " + employeeRef_extension2 + "." );
						}
						if (firstNotNull_employeeRef_extension == null && employeeRef_extension2 != null) {
							firstNotNull_employeeRef_extension = employeeRef_extension2;
						}
					}
					employeeRef_res.setExtension(firstNotNull_employeeRef_extension);
					// attribute 'Employee.firstName'
					String firstNotNull_employeeRef_firstName = Util.getStringValue(r.getAs("employeeRef_0.firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_firstName2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".firstName"));
						if (firstNotNull_employeeRef_firstName != null && employeeRef_firstName2 != null && !firstNotNull_employeeRef_firstName.equals(employeeRef_firstName2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.firstName': " + firstNotNull_employeeRef_firstName + " and " + employeeRef_firstName2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.firstName': " + firstNotNull_employeeRef_firstName + " and " + employeeRef_firstName2 + "." );
						}
						if (firstNotNull_employeeRef_firstName == null && employeeRef_firstName2 != null) {
							firstNotNull_employeeRef_firstName = employeeRef_firstName2;
						}
					}
					employeeRef_res.setFirstName(firstNotNull_employeeRef_firstName);
					// attribute 'Employee.hireDate'
					LocalDate firstNotNull_employeeRef_hireDate = Util.getLocalDateValue(r.getAs("employeeRef_0.hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate employeeRef_hireDate2 = Util.getLocalDateValue(r.getAs("employeeRef_" + i + ".hireDate"));
						if (firstNotNull_employeeRef_hireDate != null && employeeRef_hireDate2 != null && !firstNotNull_employeeRef_hireDate.equals(employeeRef_hireDate2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_employeeRef_hireDate + " and " + employeeRef_hireDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_employeeRef_hireDate + " and " + employeeRef_hireDate2 + "." );
						}
						if (firstNotNull_employeeRef_hireDate == null && employeeRef_hireDate2 != null) {
							firstNotNull_employeeRef_hireDate = employeeRef_hireDate2;
						}
					}
					employeeRef_res.setHireDate(firstNotNull_employeeRef_hireDate);
					// attribute 'Employee.homePhone'
					String firstNotNull_employeeRef_homePhone = Util.getStringValue(r.getAs("employeeRef_0.homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_homePhone2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".homePhone"));
						if (firstNotNull_employeeRef_homePhone != null && employeeRef_homePhone2 != null && !firstNotNull_employeeRef_homePhone.equals(employeeRef_homePhone2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_employeeRef_homePhone + " and " + employeeRef_homePhone2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_employeeRef_homePhone + " and " + employeeRef_homePhone2 + "." );
						}
						if (firstNotNull_employeeRef_homePhone == null && employeeRef_homePhone2 != null) {
							firstNotNull_employeeRef_homePhone = employeeRef_homePhone2;
						}
					}
					employeeRef_res.setHomePhone(firstNotNull_employeeRef_homePhone);
					// attribute 'Employee.lastName'
					String firstNotNull_employeeRef_lastName = Util.getStringValue(r.getAs("employeeRef_0.lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_lastName2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".lastName"));
						if (firstNotNull_employeeRef_lastName != null && employeeRef_lastName2 != null && !firstNotNull_employeeRef_lastName.equals(employeeRef_lastName2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.lastName': " + firstNotNull_employeeRef_lastName + " and " + employeeRef_lastName2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.lastName': " + firstNotNull_employeeRef_lastName + " and " + employeeRef_lastName2 + "." );
						}
						if (firstNotNull_employeeRef_lastName == null && employeeRef_lastName2 != null) {
							firstNotNull_employeeRef_lastName = employeeRef_lastName2;
						}
					}
					employeeRef_res.setLastName(firstNotNull_employeeRef_lastName);
					// attribute 'Employee.notes'
					String firstNotNull_employeeRef_notes = Util.getStringValue(r.getAs("employeeRef_0.notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_notes2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".notes"));
						if (firstNotNull_employeeRef_notes != null && employeeRef_notes2 != null && !firstNotNull_employeeRef_notes.equals(employeeRef_notes2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_employeeRef_notes + " and " + employeeRef_notes2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_employeeRef_notes + " and " + employeeRef_notes2 + "." );
						}
						if (firstNotNull_employeeRef_notes == null && employeeRef_notes2 != null) {
							firstNotNull_employeeRef_notes = employeeRef_notes2;
						}
					}
					employeeRef_res.setNotes(firstNotNull_employeeRef_notes);
					// attribute 'Employee.photo'
					byte[] firstNotNull_employeeRef_photo = Util.getByteArrayValue(r.getAs("employeeRef_0.photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] employeeRef_photo2 = Util.getByteArrayValue(r.getAs("employeeRef_" + i + ".photo"));
						if (firstNotNull_employeeRef_photo != null && employeeRef_photo2 != null && !firstNotNull_employeeRef_photo.equals(employeeRef_photo2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_employeeRef_photo + " and " + employeeRef_photo2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_employeeRef_photo + " and " + employeeRef_photo2 + "." );
						}
						if (firstNotNull_employeeRef_photo == null && employeeRef_photo2 != null) {
							firstNotNull_employeeRef_photo = employeeRef_photo2;
						}
					}
					employeeRef_res.setPhoto(firstNotNull_employeeRef_photo);
					// attribute 'Employee.photoPath'
					String firstNotNull_employeeRef_photoPath = Util.getStringValue(r.getAs("employeeRef_0.photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_photoPath2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".photoPath"));
						if (firstNotNull_employeeRef_photoPath != null && employeeRef_photoPath2 != null && !firstNotNull_employeeRef_photoPath.equals(employeeRef_photoPath2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_employeeRef_photoPath + " and " + employeeRef_photoPath2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_employeeRef_photoPath + " and " + employeeRef_photoPath2 + "." );
						}
						if (firstNotNull_employeeRef_photoPath == null && employeeRef_photoPath2 != null) {
							firstNotNull_employeeRef_photoPath = employeeRef_photoPath2;
						}
					}
					employeeRef_res.setPhotoPath(firstNotNull_employeeRef_photoPath);
					// attribute 'Employee.postalCode'
					String firstNotNull_employeeRef_postalCode = Util.getStringValue(r.getAs("employeeRef_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_postalCode2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".postalCode"));
						if (firstNotNull_employeeRef_postalCode != null && employeeRef_postalCode2 != null && !firstNotNull_employeeRef_postalCode.equals(employeeRef_postalCode2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_employeeRef_postalCode + " and " + employeeRef_postalCode2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_employeeRef_postalCode + " and " + employeeRef_postalCode2 + "." );
						}
						if (firstNotNull_employeeRef_postalCode == null && employeeRef_postalCode2 != null) {
							firstNotNull_employeeRef_postalCode = employeeRef_postalCode2;
						}
					}
					employeeRef_res.setPostalCode(firstNotNull_employeeRef_postalCode);
					// attribute 'Employee.region'
					String firstNotNull_employeeRef_region = Util.getStringValue(r.getAs("employeeRef_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_region2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".region"));
						if (firstNotNull_employeeRef_region != null && employeeRef_region2 != null && !firstNotNull_employeeRef_region.equals(employeeRef_region2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.region': " + firstNotNull_employeeRef_region + " and " + employeeRef_region2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.region': " + firstNotNull_employeeRef_region + " and " + employeeRef_region2 + "." );
						}
						if (firstNotNull_employeeRef_region == null && employeeRef_region2 != null) {
							firstNotNull_employeeRef_region = employeeRef_region2;
						}
					}
					employeeRef_res.setRegion(firstNotNull_employeeRef_region);
					// attribute 'Employee.salary'
					Double firstNotNull_employeeRef_salary = Util.getDoubleValue(r.getAs("employeeRef_0.salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double employeeRef_salary2 = Util.getDoubleValue(r.getAs("employeeRef_" + i + ".salary"));
						if (firstNotNull_employeeRef_salary != null && employeeRef_salary2 != null && !firstNotNull_employeeRef_salary.equals(employeeRef_salary2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_employeeRef_salary + " and " + employeeRef_salary2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_employeeRef_salary + " and " + employeeRef_salary2 + "." );
						}
						if (firstNotNull_employeeRef_salary == null && employeeRef_salary2 != null) {
							firstNotNull_employeeRef_salary = employeeRef_salary2;
						}
					}
					employeeRef_res.setSalary(firstNotNull_employeeRef_salary);
					// attribute 'Employee.title'
					String firstNotNull_employeeRef_title = Util.getStringValue(r.getAs("employeeRef_0.title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_title2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".title"));
						if (firstNotNull_employeeRef_title != null && employeeRef_title2 != null && !firstNotNull_employeeRef_title.equals(employeeRef_title2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.title': " + firstNotNull_employeeRef_title + " and " + employeeRef_title2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.title': " + firstNotNull_employeeRef_title + " and " + employeeRef_title2 + "." );
						}
						if (firstNotNull_employeeRef_title == null && employeeRef_title2 != null) {
							firstNotNull_employeeRef_title = employeeRef_title2;
						}
					}
					employeeRef_res.setTitle(firstNotNull_employeeRef_title);
					// attribute 'Employee.titleOfCourtesy'
					String firstNotNull_employeeRef_titleOfCourtesy = Util.getStringValue(r.getAs("employeeRef_0.titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String employeeRef_titleOfCourtesy2 = Util.getStringValue(r.getAs("employeeRef_" + i + ".titleOfCourtesy"));
						if (firstNotNull_employeeRef_titleOfCourtesy != null && employeeRef_titleOfCourtesy2 != null && !firstNotNull_employeeRef_titleOfCourtesy.equals(employeeRef_titleOfCourtesy2)) {
							handles_res.addLogEvent("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_employeeRef_titleOfCourtesy + " and " + employeeRef_titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employeeRef_res.getEmployeeID()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_employeeRef_titleOfCourtesy + " and " + employeeRef_titleOfCourtesy2 + "." );
						}
						if (firstNotNull_employeeRef_titleOfCourtesy == null && employeeRef_titleOfCourtesy2 != null) {
							firstNotNull_employeeRef_titleOfCourtesy = employeeRef_titleOfCourtesy2;
						}
					}
					employeeRef_res.setTitleOfCourtesy(firstNotNull_employeeRef_titleOfCourtesy);
	
					handles_res.setOrder(order_res);
					handles_res.setEmployeeRef(employeeRef_res);
					return handles_res;
		}
		, Encoders.bean(Handles.class));
	
	}
	
	//Empty arguments
	public Dataset<Handles> getHandlesList(){
		 return getHandlesList(null,null);
	}
	
	public abstract Dataset<Handles> getHandlesList(
		Condition<OrderAttribute> order_condition,
		Condition<EmployeeAttribute> employeeRef_condition);
	
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
	
	
	
	public abstract void deleteHandlesList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition);
	
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
