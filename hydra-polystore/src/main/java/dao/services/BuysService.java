package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Buys;
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


public abstract class BuysService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuysService.class);
	
	// method accessing the embedded object customer mapped to role boughtOrder
	public abstract Dataset<Buys> getBuysListInmyMongoDBOrderscustomer(Condition<OrderAttribute> boughtOrder_condition, Condition<CustomerAttribute> customerRef_condition, MutableBoolean boughtOrder_refilter, MutableBoolean customerRef_refilter);
	
	
	public static Dataset<Buys> fullLeftOuterJoinBetweenBuysAndBoughtOrder(Dataset<Buys> d1, Dataset<Order> d2) {
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
		joinCond = d1.col("boughtOrder.orderID").equalTo(d2_.col("A_orderID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Buys>) r -> {
				Buys res = new Buys();
	
				Order boughtOrder = new Order();
				Object o = r.getAs("boughtOrder");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						boughtOrder.setOrderID(Util.getIntegerValue(r2.getAs("orderID")));
						boughtOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						boughtOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						boughtOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						boughtOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						boughtOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						boughtOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						boughtOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						boughtOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						boughtOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						boughtOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						boughtOrder = (Order) o;
					}
				}
	
				res.setBoughtOrder(boughtOrder);
	
				Integer orderID = Util.getIntegerValue(r.getAs("A_orderID"));
				if (boughtOrder.getOrderID() != null && orderID != null && !boughtOrder.getOrderID().equals(orderID)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.orderID': " + boughtOrder.getOrderID() + " and " + orderID + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.orderID': " + boughtOrder.getOrderID() + " and " + orderID + "." );
				}
				if(orderID != null)
					boughtOrder.setOrderID(orderID);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (boughtOrder.getFreight() != null && freight != null && !boughtOrder.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.freight': " + boughtOrder.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.freight': " + boughtOrder.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					boughtOrder.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (boughtOrder.getOrderDate() != null && orderDate != null && !boughtOrder.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.orderDate': " + boughtOrder.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.orderDate': " + boughtOrder.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					boughtOrder.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (boughtOrder.getRequiredDate() != null && requiredDate != null && !boughtOrder.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.requiredDate': " + boughtOrder.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.requiredDate': " + boughtOrder.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					boughtOrder.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (boughtOrder.getShipAddress() != null && shipAddress != null && !boughtOrder.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shipAddress': " + boughtOrder.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shipAddress': " + boughtOrder.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					boughtOrder.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (boughtOrder.getShipCity() != null && shipCity != null && !boughtOrder.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shipCity': " + boughtOrder.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shipCity': " + boughtOrder.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					boughtOrder.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (boughtOrder.getShipCountry() != null && shipCountry != null && !boughtOrder.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shipCountry': " + boughtOrder.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shipCountry': " + boughtOrder.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					boughtOrder.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (boughtOrder.getShipName() != null && shipName != null && !boughtOrder.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shipName': " + boughtOrder.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shipName': " + boughtOrder.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					boughtOrder.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (boughtOrder.getShipPostalCode() != null && shipPostalCode != null && !boughtOrder.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shipPostalCode': " + boughtOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shipPostalCode': " + boughtOrder.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					boughtOrder.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (boughtOrder.getShipRegion() != null && shipRegion != null && !boughtOrder.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shipRegion': " + boughtOrder.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shipRegion': " + boughtOrder.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					boughtOrder.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (boughtOrder.getShippedDate() != null && shippedDate != null && !boughtOrder.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.shippedDate': " + boughtOrder.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.shippedDate': " + boughtOrder.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					boughtOrder.setShippedDate(shippedDate);
	
				o = r.getAs("customerRef");
				Customer customerRef = new Customer();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						customerRef.setID(Util.getStringValue(r2.getAs("iD")));
						customerRef.setAddress(Util.getStringValue(r2.getAs("address")));
						customerRef.setCity(Util.getStringValue(r2.getAs("city")));
						customerRef.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						customerRef.setContactName(Util.getStringValue(r2.getAs("contactName")));
						customerRef.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						customerRef.setCountry(Util.getStringValue(r2.getAs("country")));
						customerRef.setFax(Util.getStringValue(r2.getAs("fax")));
						customerRef.setPhone(Util.getStringValue(r2.getAs("phone")));
						customerRef.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						customerRef.setRegion(Util.getStringValue(r2.getAs("region")));
					} 
					if(o instanceof Customer) {
						customerRef = (Customer) o;
					}
				}
	
				res.setCustomerRef(customerRef);
	
				return res;
		}, Encoders.bean(Buys.class));
	
		
		
	}
	public static Dataset<Buys> fullLeftOuterJoinBetweenBuysAndCustomerRef(Dataset<Buys> d1, Dataset<Customer> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("iD", "A_iD")
			.withColumnRenamed("address", "A_address")
			.withColumnRenamed("city", "A_city")
			.withColumnRenamed("companyName", "A_companyName")
			.withColumnRenamed("contactName", "A_contactName")
			.withColumnRenamed("contactTitle", "A_contactTitle")
			.withColumnRenamed("country", "A_country")
			.withColumnRenamed("fax", "A_fax")
			.withColumnRenamed("phone", "A_phone")
			.withColumnRenamed("postalCode", "A_postalCode")
			.withColumnRenamed("region", "A_region")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("customerRef.iD").equalTo(d2_.col("A_iD"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Buys>) r -> {
				Buys res = new Buys();
	
				Customer customerRef = new Customer();
				Object o = r.getAs("customerRef");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						customerRef.setID(Util.getStringValue(r2.getAs("iD")));
						customerRef.setAddress(Util.getStringValue(r2.getAs("address")));
						customerRef.setCity(Util.getStringValue(r2.getAs("city")));
						customerRef.setCompanyName(Util.getStringValue(r2.getAs("companyName")));
						customerRef.setContactName(Util.getStringValue(r2.getAs("contactName")));
						customerRef.setContactTitle(Util.getStringValue(r2.getAs("contactTitle")));
						customerRef.setCountry(Util.getStringValue(r2.getAs("country")));
						customerRef.setFax(Util.getStringValue(r2.getAs("fax")));
						customerRef.setPhone(Util.getStringValue(r2.getAs("phone")));
						customerRef.setPostalCode(Util.getStringValue(r2.getAs("postalCode")));
						customerRef.setRegion(Util.getStringValue(r2.getAs("region")));
					} 
					if(o instanceof Customer) {
						customerRef = (Customer) o;
					}
				}
	
				res.setCustomerRef(customerRef);
	
				String iD = Util.getStringValue(r.getAs("A_iD"));
				if (customerRef.getID() != null && iD != null && !customerRef.getID().equals(iD)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.iD': " + customerRef.getID() + " and " + iD + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.iD': " + customerRef.getID() + " and " + iD + "." );
				}
				if(iD != null)
					customerRef.setID(iD);
				String address = Util.getStringValue(r.getAs("A_address"));
				if (customerRef.getAddress() != null && address != null && !customerRef.getAddress().equals(address)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.address': " + customerRef.getAddress() + " and " + address + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.address': " + customerRef.getAddress() + " and " + address + "." );
				}
				if(address != null)
					customerRef.setAddress(address);
				String city = Util.getStringValue(r.getAs("A_city"));
				if (customerRef.getCity() != null && city != null && !customerRef.getCity().equals(city)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.city': " + customerRef.getCity() + " and " + city + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.city': " + customerRef.getCity() + " and " + city + "." );
				}
				if(city != null)
					customerRef.setCity(city);
				String companyName = Util.getStringValue(r.getAs("A_companyName"));
				if (customerRef.getCompanyName() != null && companyName != null && !customerRef.getCompanyName().equals(companyName)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.companyName': " + customerRef.getCompanyName() + " and " + companyName + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.companyName': " + customerRef.getCompanyName() + " and " + companyName + "." );
				}
				if(companyName != null)
					customerRef.setCompanyName(companyName);
				String contactName = Util.getStringValue(r.getAs("A_contactName"));
				if (customerRef.getContactName() != null && contactName != null && !customerRef.getContactName().equals(contactName)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.contactName': " + customerRef.getContactName() + " and " + contactName + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.contactName': " + customerRef.getContactName() + " and " + contactName + "." );
				}
				if(contactName != null)
					customerRef.setContactName(contactName);
				String contactTitle = Util.getStringValue(r.getAs("A_contactTitle"));
				if (customerRef.getContactTitle() != null && contactTitle != null && !customerRef.getContactTitle().equals(contactTitle)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.contactTitle': " + customerRef.getContactTitle() + " and " + contactTitle + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.contactTitle': " + customerRef.getContactTitle() + " and " + contactTitle + "." );
				}
				if(contactTitle != null)
					customerRef.setContactTitle(contactTitle);
				String country = Util.getStringValue(r.getAs("A_country"));
				if (customerRef.getCountry() != null && country != null && !customerRef.getCountry().equals(country)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.country': " + customerRef.getCountry() + " and " + country + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.country': " + customerRef.getCountry() + " and " + country + "." );
				}
				if(country != null)
					customerRef.setCountry(country);
				String fax = Util.getStringValue(r.getAs("A_fax"));
				if (customerRef.getFax() != null && fax != null && !customerRef.getFax().equals(fax)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.fax': " + customerRef.getFax() + " and " + fax + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.fax': " + customerRef.getFax() + " and " + fax + "." );
				}
				if(fax != null)
					customerRef.setFax(fax);
				String phone = Util.getStringValue(r.getAs("A_phone"));
				if (customerRef.getPhone() != null && phone != null && !customerRef.getPhone().equals(phone)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.phone': " + customerRef.getPhone() + " and " + phone + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.phone': " + customerRef.getPhone() + " and " + phone + "." );
				}
				if(phone != null)
					customerRef.setPhone(phone);
				String postalCode = Util.getStringValue(r.getAs("A_postalCode"));
				if (customerRef.getPostalCode() != null && postalCode != null && !customerRef.getPostalCode().equals(postalCode)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.postalCode': " + customerRef.getPostalCode() + " and " + postalCode + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.postalCode': " + customerRef.getPostalCode() + " and " + postalCode + "." );
				}
				if(postalCode != null)
					customerRef.setPostalCode(postalCode);
				String region = Util.getStringValue(r.getAs("A_region"));
				if (customerRef.getRegion() != null && region != null && !customerRef.getRegion().equals(region)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.region': " + customerRef.getRegion() + " and " + region + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.region': " + customerRef.getRegion() + " and " + region + "." );
				}
				if(region != null)
					customerRef.setRegion(region);
	
				o = r.getAs("boughtOrder");
				Order boughtOrder = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						boughtOrder.setOrderID(Util.getIntegerValue(r2.getAs("orderID")));
						boughtOrder.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						boughtOrder.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						boughtOrder.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						boughtOrder.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						boughtOrder.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						boughtOrder.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						boughtOrder.setShipName(Util.getStringValue(r2.getAs("shipName")));
						boughtOrder.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						boughtOrder.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						boughtOrder.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						boughtOrder = (Order) o;
					}
				}
	
				res.setBoughtOrder(boughtOrder);
	
				return res;
		}, Encoders.bean(Buys.class));
	
		
		
	}
	
	public static Dataset<Buys> fullOuterJoinsBuys(List<Dataset<Buys>> datasetsPOJO) {
		return fullOuterJoinsBuys(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Buys> fullLeftOuterJoinsBuys(List<Dataset<Buys>> datasetsPOJO) {
		return fullOuterJoinsBuys(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Buys> fullOuterJoinsBuys(List<Dataset<Buys>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("boughtOrder.orderID");
	
		idFields.add("customerRef.iD");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Buys> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("boughtOrder_orderID_" + i, d.col("boughtOrder.orderID"))
				.withColumn("customerRef_iD_" + i, d.col("customerRef.iD"))
				.withColumnRenamed("boughtOrder", "boughtOrder_" + i)
				.withColumnRenamed("customerRef", "customerRef_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("boughtOrder_orderID_0").equalTo(rows.get(1).col("boughtOrder_orderID_1"));
		joinCond = joinCond.and(rows.get(0).col("customerRef_iD_0").equalTo(rows.get(1).col("customerRef_iD_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("boughtOrder_orderID_" + (i - 1)).equalTo(rows.get(i).col("boughtOrder_orderID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("customerRef_iD_" + (i - 1)).equalTo(rows.get(i).col("customerRef_iD_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Buys>) r -> {
				Buys buys_res = new Buys();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							buys_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							buys_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order boughtOrder_res = new Order();
					Customer customerRef_res = new Customer();
					
					// attribute 'Order.orderID'
					Integer firstNotNull_boughtOrder_orderID = Util.getIntegerValue(r.getAs("boughtOrder_0.orderID"));
					boughtOrder_res.setOrderID(firstNotNull_boughtOrder_orderID);
					// attribute 'Order.freight'
					Double firstNotNull_boughtOrder_freight = Util.getDoubleValue(r.getAs("boughtOrder_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double boughtOrder_freight2 = Util.getDoubleValue(r.getAs("boughtOrder_" + i + ".freight"));
						if (firstNotNull_boughtOrder_freight != null && boughtOrder_freight2 != null && !firstNotNull_boughtOrder_freight.equals(boughtOrder_freight2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.freight': " + firstNotNull_boughtOrder_freight + " and " + boughtOrder_freight2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.freight': " + firstNotNull_boughtOrder_freight + " and " + boughtOrder_freight2 + "." );
						}
						if (firstNotNull_boughtOrder_freight == null && boughtOrder_freight2 != null) {
							firstNotNull_boughtOrder_freight = boughtOrder_freight2;
						}
					}
					boughtOrder_res.setFreight(firstNotNull_boughtOrder_freight);
					// attribute 'Order.orderDate'
					LocalDate firstNotNull_boughtOrder_orderDate = Util.getLocalDateValue(r.getAs("boughtOrder_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boughtOrder_orderDate2 = Util.getLocalDateValue(r.getAs("boughtOrder_" + i + ".orderDate"));
						if (firstNotNull_boughtOrder_orderDate != null && boughtOrder_orderDate2 != null && !firstNotNull_boughtOrder_orderDate.equals(boughtOrder_orderDate2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_boughtOrder_orderDate + " and " + boughtOrder_orderDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_boughtOrder_orderDate + " and " + boughtOrder_orderDate2 + "." );
						}
						if (firstNotNull_boughtOrder_orderDate == null && boughtOrder_orderDate2 != null) {
							firstNotNull_boughtOrder_orderDate = boughtOrder_orderDate2;
						}
					}
					boughtOrder_res.setOrderDate(firstNotNull_boughtOrder_orderDate);
					// attribute 'Order.requiredDate'
					LocalDate firstNotNull_boughtOrder_requiredDate = Util.getLocalDateValue(r.getAs("boughtOrder_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boughtOrder_requiredDate2 = Util.getLocalDateValue(r.getAs("boughtOrder_" + i + ".requiredDate"));
						if (firstNotNull_boughtOrder_requiredDate != null && boughtOrder_requiredDate2 != null && !firstNotNull_boughtOrder_requiredDate.equals(boughtOrder_requiredDate2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_boughtOrder_requiredDate + " and " + boughtOrder_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_boughtOrder_requiredDate + " and " + boughtOrder_requiredDate2 + "." );
						}
						if (firstNotNull_boughtOrder_requiredDate == null && boughtOrder_requiredDate2 != null) {
							firstNotNull_boughtOrder_requiredDate = boughtOrder_requiredDate2;
						}
					}
					boughtOrder_res.setRequiredDate(firstNotNull_boughtOrder_requiredDate);
					// attribute 'Order.shipAddress'
					String firstNotNull_boughtOrder_shipAddress = Util.getStringValue(r.getAs("boughtOrder_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipAddress2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipAddress"));
						if (firstNotNull_boughtOrder_shipAddress != null && boughtOrder_shipAddress2 != null && !firstNotNull_boughtOrder_shipAddress.equals(boughtOrder_shipAddress2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_boughtOrder_shipAddress + " and " + boughtOrder_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_boughtOrder_shipAddress + " and " + boughtOrder_shipAddress2 + "." );
						}
						if (firstNotNull_boughtOrder_shipAddress == null && boughtOrder_shipAddress2 != null) {
							firstNotNull_boughtOrder_shipAddress = boughtOrder_shipAddress2;
						}
					}
					boughtOrder_res.setShipAddress(firstNotNull_boughtOrder_shipAddress);
					// attribute 'Order.shipCity'
					String firstNotNull_boughtOrder_shipCity = Util.getStringValue(r.getAs("boughtOrder_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipCity2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipCity"));
						if (firstNotNull_boughtOrder_shipCity != null && boughtOrder_shipCity2 != null && !firstNotNull_boughtOrder_shipCity.equals(boughtOrder_shipCity2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_boughtOrder_shipCity + " and " + boughtOrder_shipCity2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_boughtOrder_shipCity + " and " + boughtOrder_shipCity2 + "." );
						}
						if (firstNotNull_boughtOrder_shipCity == null && boughtOrder_shipCity2 != null) {
							firstNotNull_boughtOrder_shipCity = boughtOrder_shipCity2;
						}
					}
					boughtOrder_res.setShipCity(firstNotNull_boughtOrder_shipCity);
					// attribute 'Order.shipCountry'
					String firstNotNull_boughtOrder_shipCountry = Util.getStringValue(r.getAs("boughtOrder_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipCountry2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipCountry"));
						if (firstNotNull_boughtOrder_shipCountry != null && boughtOrder_shipCountry2 != null && !firstNotNull_boughtOrder_shipCountry.equals(boughtOrder_shipCountry2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_boughtOrder_shipCountry + " and " + boughtOrder_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_boughtOrder_shipCountry + " and " + boughtOrder_shipCountry2 + "." );
						}
						if (firstNotNull_boughtOrder_shipCountry == null && boughtOrder_shipCountry2 != null) {
							firstNotNull_boughtOrder_shipCountry = boughtOrder_shipCountry2;
						}
					}
					boughtOrder_res.setShipCountry(firstNotNull_boughtOrder_shipCountry);
					// attribute 'Order.shipName'
					String firstNotNull_boughtOrder_shipName = Util.getStringValue(r.getAs("boughtOrder_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipName2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipName"));
						if (firstNotNull_boughtOrder_shipName != null && boughtOrder_shipName2 != null && !firstNotNull_boughtOrder_shipName.equals(boughtOrder_shipName2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_boughtOrder_shipName + " and " + boughtOrder_shipName2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_boughtOrder_shipName + " and " + boughtOrder_shipName2 + "." );
						}
						if (firstNotNull_boughtOrder_shipName == null && boughtOrder_shipName2 != null) {
							firstNotNull_boughtOrder_shipName = boughtOrder_shipName2;
						}
					}
					boughtOrder_res.setShipName(firstNotNull_boughtOrder_shipName);
					// attribute 'Order.shipPostalCode'
					String firstNotNull_boughtOrder_shipPostalCode = Util.getStringValue(r.getAs("boughtOrder_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipPostalCode2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipPostalCode"));
						if (firstNotNull_boughtOrder_shipPostalCode != null && boughtOrder_shipPostalCode2 != null && !firstNotNull_boughtOrder_shipPostalCode.equals(boughtOrder_shipPostalCode2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_boughtOrder_shipPostalCode + " and " + boughtOrder_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_boughtOrder_shipPostalCode + " and " + boughtOrder_shipPostalCode2 + "." );
						}
						if (firstNotNull_boughtOrder_shipPostalCode == null && boughtOrder_shipPostalCode2 != null) {
							firstNotNull_boughtOrder_shipPostalCode = boughtOrder_shipPostalCode2;
						}
					}
					boughtOrder_res.setShipPostalCode(firstNotNull_boughtOrder_shipPostalCode);
					// attribute 'Order.shipRegion'
					String firstNotNull_boughtOrder_shipRegion = Util.getStringValue(r.getAs("boughtOrder_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String boughtOrder_shipRegion2 = Util.getStringValue(r.getAs("boughtOrder_" + i + ".shipRegion"));
						if (firstNotNull_boughtOrder_shipRegion != null && boughtOrder_shipRegion2 != null && !firstNotNull_boughtOrder_shipRegion.equals(boughtOrder_shipRegion2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_boughtOrder_shipRegion + " and " + boughtOrder_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_boughtOrder_shipRegion + " and " + boughtOrder_shipRegion2 + "." );
						}
						if (firstNotNull_boughtOrder_shipRegion == null && boughtOrder_shipRegion2 != null) {
							firstNotNull_boughtOrder_shipRegion = boughtOrder_shipRegion2;
						}
					}
					boughtOrder_res.setShipRegion(firstNotNull_boughtOrder_shipRegion);
					// attribute 'Order.shippedDate'
					LocalDate firstNotNull_boughtOrder_shippedDate = Util.getLocalDateValue(r.getAs("boughtOrder_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate boughtOrder_shippedDate2 = Util.getLocalDateValue(r.getAs("boughtOrder_" + i + ".shippedDate"));
						if (firstNotNull_boughtOrder_shippedDate != null && boughtOrder_shippedDate2 != null && !firstNotNull_boughtOrder_shippedDate.equals(boughtOrder_shippedDate2)) {
							buys_res.addLogEvent("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_boughtOrder_shippedDate + " and " + boughtOrder_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+boughtOrder_res.getOrderID()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_boughtOrder_shippedDate + " and " + boughtOrder_shippedDate2 + "." );
						}
						if (firstNotNull_boughtOrder_shippedDate == null && boughtOrder_shippedDate2 != null) {
							firstNotNull_boughtOrder_shippedDate = boughtOrder_shippedDate2;
						}
					}
					boughtOrder_res.setShippedDate(firstNotNull_boughtOrder_shippedDate);
					// attribute 'Customer.iD'
					String firstNotNull_customerRef_iD = Util.getStringValue(r.getAs("customerRef_0.iD"));
					customerRef_res.setID(firstNotNull_customerRef_iD);
					// attribute 'Customer.address'
					String firstNotNull_customerRef_address = Util.getStringValue(r.getAs("customerRef_0.address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_address2 = Util.getStringValue(r.getAs("customerRef_" + i + ".address"));
						if (firstNotNull_customerRef_address != null && customerRef_address2 != null && !firstNotNull_customerRef_address.equals(customerRef_address2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.address': " + firstNotNull_customerRef_address + " and " + customerRef_address2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.address': " + firstNotNull_customerRef_address + " and " + customerRef_address2 + "." );
						}
						if (firstNotNull_customerRef_address == null && customerRef_address2 != null) {
							firstNotNull_customerRef_address = customerRef_address2;
						}
					}
					customerRef_res.setAddress(firstNotNull_customerRef_address);
					// attribute 'Customer.city'
					String firstNotNull_customerRef_city = Util.getStringValue(r.getAs("customerRef_0.city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_city2 = Util.getStringValue(r.getAs("customerRef_" + i + ".city"));
						if (firstNotNull_customerRef_city != null && customerRef_city2 != null && !firstNotNull_customerRef_city.equals(customerRef_city2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.city': " + firstNotNull_customerRef_city + " and " + customerRef_city2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.city': " + firstNotNull_customerRef_city + " and " + customerRef_city2 + "." );
						}
						if (firstNotNull_customerRef_city == null && customerRef_city2 != null) {
							firstNotNull_customerRef_city = customerRef_city2;
						}
					}
					customerRef_res.setCity(firstNotNull_customerRef_city);
					// attribute 'Customer.companyName'
					String firstNotNull_customerRef_companyName = Util.getStringValue(r.getAs("customerRef_0.companyName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_companyName2 = Util.getStringValue(r.getAs("customerRef_" + i + ".companyName"));
						if (firstNotNull_customerRef_companyName != null && customerRef_companyName2 != null && !firstNotNull_customerRef_companyName.equals(customerRef_companyName2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.companyName': " + firstNotNull_customerRef_companyName + " and " + customerRef_companyName2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.companyName': " + firstNotNull_customerRef_companyName + " and " + customerRef_companyName2 + "." );
						}
						if (firstNotNull_customerRef_companyName == null && customerRef_companyName2 != null) {
							firstNotNull_customerRef_companyName = customerRef_companyName2;
						}
					}
					customerRef_res.setCompanyName(firstNotNull_customerRef_companyName);
					// attribute 'Customer.contactName'
					String firstNotNull_customerRef_contactName = Util.getStringValue(r.getAs("customerRef_0.contactName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_contactName2 = Util.getStringValue(r.getAs("customerRef_" + i + ".contactName"));
						if (firstNotNull_customerRef_contactName != null && customerRef_contactName2 != null && !firstNotNull_customerRef_contactName.equals(customerRef_contactName2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.contactName': " + firstNotNull_customerRef_contactName + " and " + customerRef_contactName2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.contactName': " + firstNotNull_customerRef_contactName + " and " + customerRef_contactName2 + "." );
						}
						if (firstNotNull_customerRef_contactName == null && customerRef_contactName2 != null) {
							firstNotNull_customerRef_contactName = customerRef_contactName2;
						}
					}
					customerRef_res.setContactName(firstNotNull_customerRef_contactName);
					// attribute 'Customer.contactTitle'
					String firstNotNull_customerRef_contactTitle = Util.getStringValue(r.getAs("customerRef_0.contactTitle"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_contactTitle2 = Util.getStringValue(r.getAs("customerRef_" + i + ".contactTitle"));
						if (firstNotNull_customerRef_contactTitle != null && customerRef_contactTitle2 != null && !firstNotNull_customerRef_contactTitle.equals(customerRef_contactTitle2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.contactTitle': " + firstNotNull_customerRef_contactTitle + " and " + customerRef_contactTitle2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.contactTitle': " + firstNotNull_customerRef_contactTitle + " and " + customerRef_contactTitle2 + "." );
						}
						if (firstNotNull_customerRef_contactTitle == null && customerRef_contactTitle2 != null) {
							firstNotNull_customerRef_contactTitle = customerRef_contactTitle2;
						}
					}
					customerRef_res.setContactTitle(firstNotNull_customerRef_contactTitle);
					// attribute 'Customer.country'
					String firstNotNull_customerRef_country = Util.getStringValue(r.getAs("customerRef_0.country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_country2 = Util.getStringValue(r.getAs("customerRef_" + i + ".country"));
						if (firstNotNull_customerRef_country != null && customerRef_country2 != null && !firstNotNull_customerRef_country.equals(customerRef_country2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.country': " + firstNotNull_customerRef_country + " and " + customerRef_country2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.country': " + firstNotNull_customerRef_country + " and " + customerRef_country2 + "." );
						}
						if (firstNotNull_customerRef_country == null && customerRef_country2 != null) {
							firstNotNull_customerRef_country = customerRef_country2;
						}
					}
					customerRef_res.setCountry(firstNotNull_customerRef_country);
					// attribute 'Customer.fax'
					String firstNotNull_customerRef_fax = Util.getStringValue(r.getAs("customerRef_0.fax"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_fax2 = Util.getStringValue(r.getAs("customerRef_" + i + ".fax"));
						if (firstNotNull_customerRef_fax != null && customerRef_fax2 != null && !firstNotNull_customerRef_fax.equals(customerRef_fax2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.fax': " + firstNotNull_customerRef_fax + " and " + customerRef_fax2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.fax': " + firstNotNull_customerRef_fax + " and " + customerRef_fax2 + "." );
						}
						if (firstNotNull_customerRef_fax == null && customerRef_fax2 != null) {
							firstNotNull_customerRef_fax = customerRef_fax2;
						}
					}
					customerRef_res.setFax(firstNotNull_customerRef_fax);
					// attribute 'Customer.phone'
					String firstNotNull_customerRef_phone = Util.getStringValue(r.getAs("customerRef_0.phone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_phone2 = Util.getStringValue(r.getAs("customerRef_" + i + ".phone"));
						if (firstNotNull_customerRef_phone != null && customerRef_phone2 != null && !firstNotNull_customerRef_phone.equals(customerRef_phone2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.phone': " + firstNotNull_customerRef_phone + " and " + customerRef_phone2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.phone': " + firstNotNull_customerRef_phone + " and " + customerRef_phone2 + "." );
						}
						if (firstNotNull_customerRef_phone == null && customerRef_phone2 != null) {
							firstNotNull_customerRef_phone = customerRef_phone2;
						}
					}
					customerRef_res.setPhone(firstNotNull_customerRef_phone);
					// attribute 'Customer.postalCode'
					String firstNotNull_customerRef_postalCode = Util.getStringValue(r.getAs("customerRef_0.postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_postalCode2 = Util.getStringValue(r.getAs("customerRef_" + i + ".postalCode"));
						if (firstNotNull_customerRef_postalCode != null && customerRef_postalCode2 != null && !firstNotNull_customerRef_postalCode.equals(customerRef_postalCode2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.postalCode': " + firstNotNull_customerRef_postalCode + " and " + customerRef_postalCode2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.postalCode': " + firstNotNull_customerRef_postalCode + " and " + customerRef_postalCode2 + "." );
						}
						if (firstNotNull_customerRef_postalCode == null && customerRef_postalCode2 != null) {
							firstNotNull_customerRef_postalCode = customerRef_postalCode2;
						}
					}
					customerRef_res.setPostalCode(firstNotNull_customerRef_postalCode);
					// attribute 'Customer.region'
					String firstNotNull_customerRef_region = Util.getStringValue(r.getAs("customerRef_0.region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String customerRef_region2 = Util.getStringValue(r.getAs("customerRef_" + i + ".region"));
						if (firstNotNull_customerRef_region != null && customerRef_region2 != null && !firstNotNull_customerRef_region.equals(customerRef_region2)) {
							buys_res.addLogEvent("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.region': " + firstNotNull_customerRef_region + " and " + customerRef_region2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customerRef_res.getID()+"]: different values found for attribute 'Customer.region': " + firstNotNull_customerRef_region + " and " + customerRef_region2 + "." );
						}
						if (firstNotNull_customerRef_region == null && customerRef_region2 != null) {
							firstNotNull_customerRef_region = customerRef_region2;
						}
					}
					customerRef_res.setRegion(firstNotNull_customerRef_region);
	
					buys_res.setBoughtOrder(boughtOrder_res);
					buys_res.setCustomerRef(customerRef_res);
					return buys_res;
		}
		, Encoders.bean(Buys.class));
	
	}
	
	//Empty arguments
	public Dataset<Buys> getBuysList(){
		 return getBuysList(null,null);
	}
	
	public abstract Dataset<Buys> getBuysList(
		Condition<OrderAttribute> boughtOrder_condition,
		Condition<CustomerAttribute> customerRef_condition);
	
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
	
	
	
	public abstract void deleteBuysList(
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition);
	
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
