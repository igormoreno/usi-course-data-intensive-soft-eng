package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.*;
import pojo.Composed_of;
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


public abstract class Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofService.class);
	
	
	// method accessing the embedded object products mapped to role productRef
	public abstract Dataset<Composed_of> getComposed_ofListInmyMongoDBOrdersproducts(Condition<ProductAttribute> productRef_condition, Condition<OrderAttribute> orderRef_condition, MutableBoolean productRef_refilter, MutableBoolean orderRef_refilter);
	
	public static Dataset<Composed_of> fullLeftOuterJoinBetweenComposed_ofAndOrderRef(Dataset<Composed_of> d1, Dataset<Order> d2) {
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
		joinCond = d1.col("orderRef.orderID").equalTo(d2_.col("A_orderID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Composed_of>) r -> {
				Composed_of res = new Composed_of();
				res.setUnitPrice(r.getAs("unitPrice"));
				res.setQuantity(r.getAs("quantity"));
				res.setDiscount(r.getAs("discount"));
	
				Order orderRef = new Order();
				Object o = r.getAs("orderRef");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderRef.setOrderID(Util.getIntegerValue(r2.getAs("orderID")));
						orderRef.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						orderRef.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						orderRef.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						orderRef.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						orderRef.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						orderRef.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						orderRef.setShipName(Util.getStringValue(r2.getAs("shipName")));
						orderRef.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						orderRef.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						orderRef.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						orderRef = (Order) o;
					}
				}
	
				res.setOrderRef(orderRef);
	
				Integer orderID = Util.getIntegerValue(r.getAs("A_orderID"));
				if (orderRef.getOrderID() != null && orderID != null && !orderRef.getOrderID().equals(orderID)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderID': " + orderRef.getOrderID() + " and " + orderID + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderID': " + orderRef.getOrderID() + " and " + orderID + "." );
				}
				if(orderID != null)
					orderRef.setOrderID(orderID);
				Double freight = Util.getDoubleValue(r.getAs("A_freight"));
				if (orderRef.getFreight() != null && freight != null && !orderRef.getFreight().equals(freight)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.freight': " + orderRef.getFreight() + " and " + freight + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.freight': " + orderRef.getFreight() + " and " + freight + "." );
				}
				if(freight != null)
					orderRef.setFreight(freight);
				LocalDate orderDate = Util.getLocalDateValue(r.getAs("A_orderDate"));
				if (orderRef.getOrderDate() != null && orderDate != null && !orderRef.getOrderDate().equals(orderDate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderDate': " + orderRef.getOrderDate() + " and " + orderDate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderDate': " + orderRef.getOrderDate() + " and " + orderDate + "." );
				}
				if(orderDate != null)
					orderRef.setOrderDate(orderDate);
				LocalDate requiredDate = Util.getLocalDateValue(r.getAs("A_requiredDate"));
				if (orderRef.getRequiredDate() != null && requiredDate != null && !orderRef.getRequiredDate().equals(requiredDate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.requiredDate': " + orderRef.getRequiredDate() + " and " + requiredDate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.requiredDate': " + orderRef.getRequiredDate() + " and " + requiredDate + "." );
				}
				if(requiredDate != null)
					orderRef.setRequiredDate(requiredDate);
				String shipAddress = Util.getStringValue(r.getAs("A_shipAddress"));
				if (orderRef.getShipAddress() != null && shipAddress != null && !orderRef.getShipAddress().equals(shipAddress)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipAddress': " + orderRef.getShipAddress() + " and " + shipAddress + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipAddress': " + orderRef.getShipAddress() + " and " + shipAddress + "." );
				}
				if(shipAddress != null)
					orderRef.setShipAddress(shipAddress);
				String shipCity = Util.getStringValue(r.getAs("A_shipCity"));
				if (orderRef.getShipCity() != null && shipCity != null && !orderRef.getShipCity().equals(shipCity)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCity': " + orderRef.getShipCity() + " and " + shipCity + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCity': " + orderRef.getShipCity() + " and " + shipCity + "." );
				}
				if(shipCity != null)
					orderRef.setShipCity(shipCity);
				String shipCountry = Util.getStringValue(r.getAs("A_shipCountry"));
				if (orderRef.getShipCountry() != null && shipCountry != null && !orderRef.getShipCountry().equals(shipCountry)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCountry': " + orderRef.getShipCountry() + " and " + shipCountry + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipCountry': " + orderRef.getShipCountry() + " and " + shipCountry + "." );
				}
				if(shipCountry != null)
					orderRef.setShipCountry(shipCountry);
				String shipName = Util.getStringValue(r.getAs("A_shipName"));
				if (orderRef.getShipName() != null && shipName != null && !orderRef.getShipName().equals(shipName)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipName': " + orderRef.getShipName() + " and " + shipName + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipName': " + orderRef.getShipName() + " and " + shipName + "." );
				}
				if(shipName != null)
					orderRef.setShipName(shipName);
				String shipPostalCode = Util.getStringValue(r.getAs("A_shipPostalCode"));
				if (orderRef.getShipPostalCode() != null && shipPostalCode != null && !orderRef.getShipPostalCode().equals(shipPostalCode)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipPostalCode': " + orderRef.getShipPostalCode() + " and " + shipPostalCode + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipPostalCode': " + orderRef.getShipPostalCode() + " and " + shipPostalCode + "." );
				}
				if(shipPostalCode != null)
					orderRef.setShipPostalCode(shipPostalCode);
				String shipRegion = Util.getStringValue(r.getAs("A_shipRegion"));
				if (orderRef.getShipRegion() != null && shipRegion != null && !orderRef.getShipRegion().equals(shipRegion)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipRegion': " + orderRef.getShipRegion() + " and " + shipRegion + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shipRegion': " + orderRef.getShipRegion() + " and " + shipRegion + "." );
				}
				if(shipRegion != null)
					orderRef.setShipRegion(shipRegion);
				LocalDate shippedDate = Util.getLocalDateValue(r.getAs("A_shippedDate"));
				if (orderRef.getShippedDate() != null && shippedDate != null && !orderRef.getShippedDate().equals(shippedDate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shippedDate': " + orderRef.getShippedDate() + " and " + shippedDate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.shippedDate': " + orderRef.getShippedDate() + " and " + shippedDate + "." );
				}
				if(shippedDate != null)
					orderRef.setShippedDate(shippedDate);
	
				o = r.getAs("productRef");
				Product productRef = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						productRef.setProductID(Util.getIntegerValue(r2.getAs("productID")));
						productRef.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						productRef.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						productRef.setProductName(Util.getStringValue(r2.getAs("productName")));
						productRef.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						productRef.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						productRef.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						productRef.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Product) {
						productRef = (Product) o;
					}
				}
	
				res.setProductRef(productRef);
	
				return res;
		}, Encoders.bean(Composed_of.class));
	
		
		
	}
	public static Dataset<Composed_of> fullLeftOuterJoinBetweenComposed_ofAndProductRef(Dataset<Composed_of> d1, Dataset<Product> d2) {
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
		joinCond = d1.col("productRef.productID").equalTo(d2_.col("A_productID"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map((MapFunction<Row, Composed_of>) r -> {
				Composed_of res = new Composed_of();
				res.setUnitPrice(r.getAs("unitPrice"));
				res.setQuantity(r.getAs("quantity"));
				res.setDiscount(r.getAs("discount"));
	
				Product productRef = new Product();
				Object o = r.getAs("productRef");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						productRef.setProductID(Util.getIntegerValue(r2.getAs("productID")));
						productRef.setUnitsInStock(Util.getIntegerValue(r2.getAs("unitsInStock")));
						productRef.setUnitsOnOrder(Util.getIntegerValue(r2.getAs("unitsOnOrder")));
						productRef.setProductName(Util.getStringValue(r2.getAs("productName")));
						productRef.setQuantityPerUnit(Util.getStringValue(r2.getAs("quantityPerUnit")));
						productRef.setUnitPrice(Util.getDoubleValue(r2.getAs("unitPrice")));
						productRef.setReorderLevel(Util.getIntegerValue(r2.getAs("reorderLevel")));
						productRef.setDiscontinued(Util.getBooleanValue(r2.getAs("discontinued")));
					} 
					if(o instanceof Product) {
						productRef = (Product) o;
					}
				}
	
				res.setProductRef(productRef);
	
				Integer productID = Util.getIntegerValue(r.getAs("A_productID"));
				if (productRef.getProductID() != null && productID != null && !productRef.getProductID().equals(productID)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.productID': " + productRef.getProductID() + " and " + productID + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.productID': " + productRef.getProductID() + " and " + productID + "." );
				}
				if(productID != null)
					productRef.setProductID(productID);
				Integer unitsInStock = Util.getIntegerValue(r.getAs("A_unitsInStock"));
				if (productRef.getUnitsInStock() != null && unitsInStock != null && !productRef.getUnitsInStock().equals(unitsInStock)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsInStock': " + productRef.getUnitsInStock() + " and " + unitsInStock + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsInStock': " + productRef.getUnitsInStock() + " and " + unitsInStock + "." );
				}
				if(unitsInStock != null)
					productRef.setUnitsInStock(unitsInStock);
				Integer unitsOnOrder = Util.getIntegerValue(r.getAs("A_unitsOnOrder"));
				if (productRef.getUnitsOnOrder() != null && unitsOnOrder != null && !productRef.getUnitsOnOrder().equals(unitsOnOrder)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsOnOrder': " + productRef.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitsOnOrder': " + productRef.getUnitsOnOrder() + " and " + unitsOnOrder + "." );
				}
				if(unitsOnOrder != null)
					productRef.setUnitsOnOrder(unitsOnOrder);
				String productName = Util.getStringValue(r.getAs("A_productName"));
				if (productRef.getProductName() != null && productName != null && !productRef.getProductName().equals(productName)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.productName': " + productRef.getProductName() + " and " + productName + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.productName': " + productRef.getProductName() + " and " + productName + "." );
				}
				if(productName != null)
					productRef.setProductName(productName);
				String quantityPerUnit = Util.getStringValue(r.getAs("A_quantityPerUnit"));
				if (productRef.getQuantityPerUnit() != null && quantityPerUnit != null && !productRef.getQuantityPerUnit().equals(quantityPerUnit)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantityPerUnit': " + productRef.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantityPerUnit': " + productRef.getQuantityPerUnit() + " and " + quantityPerUnit + "." );
				}
				if(quantityPerUnit != null)
					productRef.setQuantityPerUnit(quantityPerUnit);
				Double unitPrice = Util.getDoubleValue(r.getAs("A_unitPrice"));
				if (productRef.getUnitPrice() != null && unitPrice != null && !productRef.getUnitPrice().equals(unitPrice)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + productRef.getUnitPrice() + " and " + unitPrice + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + productRef.getUnitPrice() + " and " + unitPrice + "." );
				}
				if(unitPrice != null)
					productRef.setUnitPrice(unitPrice);
				Integer reorderLevel = Util.getIntegerValue(r.getAs("A_reorderLevel"));
				if (productRef.getReorderLevel() != null && reorderLevel != null && !productRef.getReorderLevel().equals(reorderLevel)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.reorderLevel': " + productRef.getReorderLevel() + " and " + reorderLevel + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.reorderLevel': " + productRef.getReorderLevel() + " and " + reorderLevel + "." );
				}
				if(reorderLevel != null)
					productRef.setReorderLevel(reorderLevel);
				Boolean discontinued = Util.getBooleanValue(r.getAs("A_discontinued"));
				if (productRef.getDiscontinued() != null && discontinued != null && !productRef.getDiscontinued().equals(discontinued)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discontinued': " + productRef.getDiscontinued() + " and " + discontinued + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discontinued': " + productRef.getDiscontinued() + " and " + discontinued + "." );
				}
				if(discontinued != null)
					productRef.setDiscontinued(discontinued);
	
				o = r.getAs("orderRef");
				Order orderRef = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderRef.setOrderID(Util.getIntegerValue(r2.getAs("orderID")));
						orderRef.setFreight(Util.getDoubleValue(r2.getAs("freight")));
						orderRef.setOrderDate(Util.getLocalDateValue(r2.getAs("orderDate")));
						orderRef.setRequiredDate(Util.getLocalDateValue(r2.getAs("requiredDate")));
						orderRef.setShipAddress(Util.getStringValue(r2.getAs("shipAddress")));
						orderRef.setShipCity(Util.getStringValue(r2.getAs("shipCity")));
						orderRef.setShipCountry(Util.getStringValue(r2.getAs("shipCountry")));
						orderRef.setShipName(Util.getStringValue(r2.getAs("shipName")));
						orderRef.setShipPostalCode(Util.getStringValue(r2.getAs("shipPostalCode")));
						orderRef.setShipRegion(Util.getStringValue(r2.getAs("shipRegion")));
						orderRef.setShippedDate(Util.getLocalDateValue(r2.getAs("shippedDate")));
					} 
					if(o instanceof Order) {
						orderRef = (Order) o;
					}
				}
	
				res.setOrderRef(orderRef);
	
				return res;
		}, Encoders.bean(Composed_of.class));
	
		
		
	}
	
	public static Dataset<Composed_of> fullOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO) {
		return fullOuterJoinsComposed_of(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Composed_of> fullLeftOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO) {
		return fullOuterJoinsComposed_of(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Composed_of> fullOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("orderRef.orderID");
	
		idFields.add("productRef.productID");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Composed_of> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("orderRef_orderID_" + i, d.col("orderRef.orderID"))
				.withColumn("productRef_productID_" + i, d.col("productRef.productID"))
				.withColumnRenamed("unitPrice", "unitPrice_" + i)
				.withColumnRenamed("quantity", "quantity_" + i)
				.withColumnRenamed("discount", "discount_" + i)
				.withColumnRenamed("orderRef", "orderRef_" + i)
				.withColumnRenamed("productRef", "productRef_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("orderRef_orderID_0").equalTo(rows.get(1).col("orderRef_orderID_1"));
		joinCond = joinCond.and(rows.get(0).col("productRef_productID_0").equalTo(rows.get(1).col("productRef_productID_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("orderRef_orderID_" + (i - 1)).equalTo(rows.get(i).col("orderRef_orderID_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("productRef_productID_" + (i - 1)).equalTo(rows.get(i).col("productRef_productID_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map((MapFunction<Row, Composed_of>) r -> {
				Composed_of composed_of_res = new Composed_of();
					
					// attribute 'Composed_of.unitPrice'
					Double firstNotNull_unitPrice = Util.getDoubleValue(r.getAs("unitPrice_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_unitPrice != null && unitPrice2 != null && !firstNotNull_unitPrice.equals(unitPrice2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_unitPrice == null && unitPrice2 != null) {
							firstNotNull_unitPrice = unitPrice2;
						}
					}
					composed_of_res.setUnitPrice(firstNotNull_unitPrice);
					
					// attribute 'Composed_of.quantity'
					Integer firstNotNull_quantity = Util.getIntegerValue(r.getAs("quantity_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer quantity2 = Util.getIntegerValue(r.getAs("quantity_" + i));
						if (firstNotNull_quantity != null && quantity2 != null && !firstNotNull_quantity.equals(quantity2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
							logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
						}
						if (firstNotNull_quantity == null && quantity2 != null) {
							firstNotNull_quantity = quantity2;
						}
					}
					composed_of_res.setQuantity(firstNotNull_quantity);
					
					// attribute 'Composed_of.discount'
					Double firstNotNull_discount = Util.getDoubleValue(r.getAs("discount_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double discount2 = Util.getDoubleValue(r.getAs("discount_" + i));
						if (firstNotNull_discount != null && discount2 != null && !firstNotNull_discount.equals(discount2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discount': " + firstNotNull_discount + " and " + discount2 + "." );
							logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.discount': " + firstNotNull_discount + " and " + discount2 + "." );
						}
						if (firstNotNull_discount == null && discount2 != null) {
							firstNotNull_discount = discount2;
						}
					}
					composed_of_res.setDiscount(firstNotNull_discount);
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							composed_of_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							composed_of_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order orderRef_res = new Order();
					Product productRef_res = new Product();
					
					// attribute 'Order.orderID'
					Integer firstNotNull_orderRef_orderID = Util.getIntegerValue(r.getAs("orderRef_0.orderID"));
					orderRef_res.setOrderID(firstNotNull_orderRef_orderID);
					// attribute 'Order.freight'
					Double firstNotNull_orderRef_freight = Util.getDoubleValue(r.getAs("orderRef_0.freight"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double orderRef_freight2 = Util.getDoubleValue(r.getAs("orderRef_" + i + ".freight"));
						if (firstNotNull_orderRef_freight != null && orderRef_freight2 != null && !firstNotNull_orderRef_freight.equals(orderRef_freight2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.freight': " + firstNotNull_orderRef_freight + " and " + orderRef_freight2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.freight': " + firstNotNull_orderRef_freight + " and " + orderRef_freight2 + "." );
						}
						if (firstNotNull_orderRef_freight == null && orderRef_freight2 != null) {
							firstNotNull_orderRef_freight = orderRef_freight2;
						}
					}
					orderRef_res.setFreight(firstNotNull_orderRef_freight);
					// attribute 'Order.orderDate'
					LocalDate firstNotNull_orderRef_orderDate = Util.getLocalDateValue(r.getAs("orderRef_0.orderDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate orderRef_orderDate2 = Util.getLocalDateValue(r.getAs("orderRef_" + i + ".orderDate"));
						if (firstNotNull_orderRef_orderDate != null && orderRef_orderDate2 != null && !firstNotNull_orderRef_orderDate.equals(orderRef_orderDate2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_orderRef_orderDate + " and " + orderRef_orderDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.orderDate': " + firstNotNull_orderRef_orderDate + " and " + orderRef_orderDate2 + "." );
						}
						if (firstNotNull_orderRef_orderDate == null && orderRef_orderDate2 != null) {
							firstNotNull_orderRef_orderDate = orderRef_orderDate2;
						}
					}
					orderRef_res.setOrderDate(firstNotNull_orderRef_orderDate);
					// attribute 'Order.requiredDate'
					LocalDate firstNotNull_orderRef_requiredDate = Util.getLocalDateValue(r.getAs("orderRef_0.requiredDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate orderRef_requiredDate2 = Util.getLocalDateValue(r.getAs("orderRef_" + i + ".requiredDate"));
						if (firstNotNull_orderRef_requiredDate != null && orderRef_requiredDate2 != null && !firstNotNull_orderRef_requiredDate.equals(orderRef_requiredDate2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_orderRef_requiredDate + " and " + orderRef_requiredDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.requiredDate': " + firstNotNull_orderRef_requiredDate + " and " + orderRef_requiredDate2 + "." );
						}
						if (firstNotNull_orderRef_requiredDate == null && orderRef_requiredDate2 != null) {
							firstNotNull_orderRef_requiredDate = orderRef_requiredDate2;
						}
					}
					orderRef_res.setRequiredDate(firstNotNull_orderRef_requiredDate);
					// attribute 'Order.shipAddress'
					String firstNotNull_orderRef_shipAddress = Util.getStringValue(r.getAs("orderRef_0.shipAddress"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderRef_shipAddress2 = Util.getStringValue(r.getAs("orderRef_" + i + ".shipAddress"));
						if (firstNotNull_orderRef_shipAddress != null && orderRef_shipAddress2 != null && !firstNotNull_orderRef_shipAddress.equals(orderRef_shipAddress2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_orderRef_shipAddress + " and " + orderRef_shipAddress2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipAddress': " + firstNotNull_orderRef_shipAddress + " and " + orderRef_shipAddress2 + "." );
						}
						if (firstNotNull_orderRef_shipAddress == null && orderRef_shipAddress2 != null) {
							firstNotNull_orderRef_shipAddress = orderRef_shipAddress2;
						}
					}
					orderRef_res.setShipAddress(firstNotNull_orderRef_shipAddress);
					// attribute 'Order.shipCity'
					String firstNotNull_orderRef_shipCity = Util.getStringValue(r.getAs("orderRef_0.shipCity"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderRef_shipCity2 = Util.getStringValue(r.getAs("orderRef_" + i + ".shipCity"));
						if (firstNotNull_orderRef_shipCity != null && orderRef_shipCity2 != null && !firstNotNull_orderRef_shipCity.equals(orderRef_shipCity2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_orderRef_shipCity + " and " + orderRef_shipCity2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipCity': " + firstNotNull_orderRef_shipCity + " and " + orderRef_shipCity2 + "." );
						}
						if (firstNotNull_orderRef_shipCity == null && orderRef_shipCity2 != null) {
							firstNotNull_orderRef_shipCity = orderRef_shipCity2;
						}
					}
					orderRef_res.setShipCity(firstNotNull_orderRef_shipCity);
					// attribute 'Order.shipCountry'
					String firstNotNull_orderRef_shipCountry = Util.getStringValue(r.getAs("orderRef_0.shipCountry"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderRef_shipCountry2 = Util.getStringValue(r.getAs("orderRef_" + i + ".shipCountry"));
						if (firstNotNull_orderRef_shipCountry != null && orderRef_shipCountry2 != null && !firstNotNull_orderRef_shipCountry.equals(orderRef_shipCountry2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_orderRef_shipCountry + " and " + orderRef_shipCountry2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipCountry': " + firstNotNull_orderRef_shipCountry + " and " + orderRef_shipCountry2 + "." );
						}
						if (firstNotNull_orderRef_shipCountry == null && orderRef_shipCountry2 != null) {
							firstNotNull_orderRef_shipCountry = orderRef_shipCountry2;
						}
					}
					orderRef_res.setShipCountry(firstNotNull_orderRef_shipCountry);
					// attribute 'Order.shipName'
					String firstNotNull_orderRef_shipName = Util.getStringValue(r.getAs("orderRef_0.shipName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderRef_shipName2 = Util.getStringValue(r.getAs("orderRef_" + i + ".shipName"));
						if (firstNotNull_orderRef_shipName != null && orderRef_shipName2 != null && !firstNotNull_orderRef_shipName.equals(orderRef_shipName2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_orderRef_shipName + " and " + orderRef_shipName2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipName': " + firstNotNull_orderRef_shipName + " and " + orderRef_shipName2 + "." );
						}
						if (firstNotNull_orderRef_shipName == null && orderRef_shipName2 != null) {
							firstNotNull_orderRef_shipName = orderRef_shipName2;
						}
					}
					orderRef_res.setShipName(firstNotNull_orderRef_shipName);
					// attribute 'Order.shipPostalCode'
					String firstNotNull_orderRef_shipPostalCode = Util.getStringValue(r.getAs("orderRef_0.shipPostalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderRef_shipPostalCode2 = Util.getStringValue(r.getAs("orderRef_" + i + ".shipPostalCode"));
						if (firstNotNull_orderRef_shipPostalCode != null && orderRef_shipPostalCode2 != null && !firstNotNull_orderRef_shipPostalCode.equals(orderRef_shipPostalCode2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_orderRef_shipPostalCode + " and " + orderRef_shipPostalCode2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipPostalCode': " + firstNotNull_orderRef_shipPostalCode + " and " + orderRef_shipPostalCode2 + "." );
						}
						if (firstNotNull_orderRef_shipPostalCode == null && orderRef_shipPostalCode2 != null) {
							firstNotNull_orderRef_shipPostalCode = orderRef_shipPostalCode2;
						}
					}
					orderRef_res.setShipPostalCode(firstNotNull_orderRef_shipPostalCode);
					// attribute 'Order.shipRegion'
					String firstNotNull_orderRef_shipRegion = Util.getStringValue(r.getAs("orderRef_0.shipRegion"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String orderRef_shipRegion2 = Util.getStringValue(r.getAs("orderRef_" + i + ".shipRegion"));
						if (firstNotNull_orderRef_shipRegion != null && orderRef_shipRegion2 != null && !firstNotNull_orderRef_shipRegion.equals(orderRef_shipRegion2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_orderRef_shipRegion + " and " + orderRef_shipRegion2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shipRegion': " + firstNotNull_orderRef_shipRegion + " and " + orderRef_shipRegion2 + "." );
						}
						if (firstNotNull_orderRef_shipRegion == null && orderRef_shipRegion2 != null) {
							firstNotNull_orderRef_shipRegion = orderRef_shipRegion2;
						}
					}
					orderRef_res.setShipRegion(firstNotNull_orderRef_shipRegion);
					// attribute 'Order.shippedDate'
					LocalDate firstNotNull_orderRef_shippedDate = Util.getLocalDateValue(r.getAs("orderRef_0.shippedDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate orderRef_shippedDate2 = Util.getLocalDateValue(r.getAs("orderRef_" + i + ".shippedDate"));
						if (firstNotNull_orderRef_shippedDate != null && orderRef_shippedDate2 != null && !firstNotNull_orderRef_shippedDate.equals(orderRef_shippedDate2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_orderRef_shippedDate + " and " + orderRef_shippedDate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+orderRef_res.getOrderID()+"]: different values found for attribute 'Order.shippedDate': " + firstNotNull_orderRef_shippedDate + " and " + orderRef_shippedDate2 + "." );
						}
						if (firstNotNull_orderRef_shippedDate == null && orderRef_shippedDate2 != null) {
							firstNotNull_orderRef_shippedDate = orderRef_shippedDate2;
						}
					}
					orderRef_res.setShippedDate(firstNotNull_orderRef_shippedDate);
					// attribute 'Product.productID'
					Integer firstNotNull_productRef_productID = Util.getIntegerValue(r.getAs("productRef_0.productID"));
					productRef_res.setProductID(firstNotNull_productRef_productID);
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_productRef_unitsInStock = Util.getIntegerValue(r.getAs("productRef_0.unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer productRef_unitsInStock2 = Util.getIntegerValue(r.getAs("productRef_" + i + ".unitsInStock"));
						if (firstNotNull_productRef_unitsInStock != null && productRef_unitsInStock2 != null && !firstNotNull_productRef_unitsInStock.equals(productRef_unitsInStock2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_productRef_unitsInStock + " and " + productRef_unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_productRef_unitsInStock + " and " + productRef_unitsInStock2 + "." );
						}
						if (firstNotNull_productRef_unitsInStock == null && productRef_unitsInStock2 != null) {
							firstNotNull_productRef_unitsInStock = productRef_unitsInStock2;
						}
					}
					productRef_res.setUnitsInStock(firstNotNull_productRef_unitsInStock);
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_productRef_unitsOnOrder = Util.getIntegerValue(r.getAs("productRef_0.unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer productRef_unitsOnOrder2 = Util.getIntegerValue(r.getAs("productRef_" + i + ".unitsOnOrder"));
						if (firstNotNull_productRef_unitsOnOrder != null && productRef_unitsOnOrder2 != null && !firstNotNull_productRef_unitsOnOrder.equals(productRef_unitsOnOrder2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_productRef_unitsOnOrder + " and " + productRef_unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_productRef_unitsOnOrder + " and " + productRef_unitsOnOrder2 + "." );
						}
						if (firstNotNull_productRef_unitsOnOrder == null && productRef_unitsOnOrder2 != null) {
							firstNotNull_productRef_unitsOnOrder = productRef_unitsOnOrder2;
						}
					}
					productRef_res.setUnitsOnOrder(firstNotNull_productRef_unitsOnOrder);
					// attribute 'Product.productName'
					String firstNotNull_productRef_productName = Util.getStringValue(r.getAs("productRef_0.productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String productRef_productName2 = Util.getStringValue(r.getAs("productRef_" + i + ".productName"));
						if (firstNotNull_productRef_productName != null && productRef_productName2 != null && !firstNotNull_productRef_productName.equals(productRef_productName2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.productName': " + firstNotNull_productRef_productName + " and " + productRef_productName2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.productName': " + firstNotNull_productRef_productName + " and " + productRef_productName2 + "." );
						}
						if (firstNotNull_productRef_productName == null && productRef_productName2 != null) {
							firstNotNull_productRef_productName = productRef_productName2;
						}
					}
					productRef_res.setProductName(firstNotNull_productRef_productName);
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_productRef_quantityPerUnit = Util.getStringValue(r.getAs("productRef_0.quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String productRef_quantityPerUnit2 = Util.getStringValue(r.getAs("productRef_" + i + ".quantityPerUnit"));
						if (firstNotNull_productRef_quantityPerUnit != null && productRef_quantityPerUnit2 != null && !firstNotNull_productRef_quantityPerUnit.equals(productRef_quantityPerUnit2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_productRef_quantityPerUnit + " and " + productRef_quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_productRef_quantityPerUnit + " and " + productRef_quantityPerUnit2 + "." );
						}
						if (firstNotNull_productRef_quantityPerUnit == null && productRef_quantityPerUnit2 != null) {
							firstNotNull_productRef_quantityPerUnit = productRef_quantityPerUnit2;
						}
					}
					productRef_res.setQuantityPerUnit(firstNotNull_productRef_quantityPerUnit);
					// attribute 'Product.unitPrice'
					Double firstNotNull_productRef_unitPrice = Util.getDoubleValue(r.getAs("productRef_0.unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double productRef_unitPrice2 = Util.getDoubleValue(r.getAs("productRef_" + i + ".unitPrice"));
						if (firstNotNull_productRef_unitPrice != null && productRef_unitPrice2 != null && !firstNotNull_productRef_unitPrice.equals(productRef_unitPrice2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_productRef_unitPrice + " and " + productRef_unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_productRef_unitPrice + " and " + productRef_unitPrice2 + "." );
						}
						if (firstNotNull_productRef_unitPrice == null && productRef_unitPrice2 != null) {
							firstNotNull_productRef_unitPrice = productRef_unitPrice2;
						}
					}
					productRef_res.setUnitPrice(firstNotNull_productRef_unitPrice);
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_productRef_reorderLevel = Util.getIntegerValue(r.getAs("productRef_0.reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer productRef_reorderLevel2 = Util.getIntegerValue(r.getAs("productRef_" + i + ".reorderLevel"));
						if (firstNotNull_productRef_reorderLevel != null && productRef_reorderLevel2 != null && !firstNotNull_productRef_reorderLevel.equals(productRef_reorderLevel2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_productRef_reorderLevel + " and " + productRef_reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_productRef_reorderLevel + " and " + productRef_reorderLevel2 + "." );
						}
						if (firstNotNull_productRef_reorderLevel == null && productRef_reorderLevel2 != null) {
							firstNotNull_productRef_reorderLevel = productRef_reorderLevel2;
						}
					}
					productRef_res.setReorderLevel(firstNotNull_productRef_reorderLevel);
					// attribute 'Product.discontinued'
					Boolean firstNotNull_productRef_discontinued = Util.getBooleanValue(r.getAs("productRef_0.discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean productRef_discontinued2 = Util.getBooleanValue(r.getAs("productRef_" + i + ".discontinued"));
						if (firstNotNull_productRef_discontinued != null && productRef_discontinued2 != null && !firstNotNull_productRef_discontinued.equals(productRef_discontinued2)) {
							composed_of_res.addLogEvent("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_productRef_discontinued + " and " + productRef_discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+productRef_res.getProductID()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_productRef_discontinued + " and " + productRef_discontinued2 + "." );
						}
						if (firstNotNull_productRef_discontinued == null && productRef_discontinued2 != null) {
							firstNotNull_productRef_discontinued = productRef_discontinued2;
						}
					}
					productRef_res.setDiscontinued(firstNotNull_productRef_discontinued);
	
					composed_of_res.setOrderRef(orderRef_res);
					composed_of_res.setProductRef(productRef_res);
					return composed_of_res;
		}
		, Encoders.bean(Composed_of.class));
	
	}
	
	//Empty arguments
	public Dataset<Composed_of> getComposed_ofList(){
		 return getComposed_ofList(null,null,null);
	}
	
	public abstract Dataset<Composed_of> getComposed_ofList(
		Condition<OrderAttribute> orderRef_condition,
		Condition<ProductAttribute> productRef_condition,
		Condition<Composed_ofAttribute> composed_of_condition
	);
	
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
	
	public abstract void insertComposed_of(Composed_of composed_of);
	
	
	public 	abstract boolean insertComposed_ofInEmbeddedStructOrdersInMyMongoDB(Composed_of composed_of);
	
	
	 public void insertComposed_of(Order orderRef ,Product productRef ){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrderRef(orderRef);
		composed_of.setProductRef(productRef);
		insertComposed_of(composed_of);
	}
	
	 public void insertComposed_of(Product product, List<Order> orderRefList){
		Composed_of composed_of = new Composed_of();
		composed_of.setProductRef(product);
		for(Order orderRef : orderRefList){
			composed_of.setOrderRef(orderRef);
			insertComposed_of(composed_of);
		}
	}
	 public void insertComposed_of(Order order, List<Product> productRefList){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrderRef(order);
		for(Product productRef : productRefList){
			composed_of.setProductRef(productRef);
			insertComposed_of(composed_of);
		}
	}
	
	public abstract void updateComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.Composed_ofAttribute> set
	);
	
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
	
	public abstract void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition);
	
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
