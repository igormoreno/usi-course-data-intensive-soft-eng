package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Order;
import conditions.*;
import dao.services.OrderService;
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


public class OrderServiceImpl extends OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInOrdersFromMyMongoDB(conditions.SetClause<OrderAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<OrderAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<OrderAttribute, Object> e : clause.entrySet()) {
				OrderAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == OrderAttribute.orderID ) {
					String fieldName = "OrderID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.freight ) {
					String fieldName = "Freight";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.orderDate ) {
					String fieldName = "OrderDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.requiredDate ) {
					String fieldName = "RequiredDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipAddress ) {
					String fieldName = "ShipAddress";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipCity ) {
					String fieldName = "ShipCity";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipCountry ) {
					String fieldName = "ShipCountry";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipName ) {
					String fieldName = "ShipName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipPostalCode ) {
					String fieldName = "ShipPostalCode";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shipRegion ) {
					String fieldName = "ShipRegion";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == OrderAttribute.shippedDate ) {
					String fieldName = "ShippedDate";
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
	
	public static String getBSONMatchQueryInOrdersFromMyMongoDB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				OrderAttribute attr = ((SimpleCondition<OrderAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrderAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrderAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrderAttribute.orderID ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "OrderID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.freight ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Freight': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.orderDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "OrderDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.requiredDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "RequiredDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipAddress ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipAddress': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipCity ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipCity': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipCountry ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipCountry': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipPostalCode ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipPostalCode': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shipRegion ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShipRegion': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == OrderAttribute.shippedDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "ShippedDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInOrdersFromMyMongoDB(Condition<OrderAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				OrderAttribute attr = ((SimpleCondition<OrderAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrderAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrderAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrderAttribute.orderID ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "OrderID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.freight ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Freight': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.orderDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "OrderDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.requiredDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "RequiredDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipAddress ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipAddress': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipCity ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipCity': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipCountry ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipCountry': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipPostalCode ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipPostalCode': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shipRegion ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShipRegion': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == OrderAttribute.shippedDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "ShippedDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
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
	
	
	
	public Dataset<Order> getOrderListInOrdersFromMyMongoDB(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Orders", bsonQuery);
	
		Dataset<Order> res = dataset.flatMap((FlatMapFunction<Row, Order>) r -> {
				Set<Order> list_res = new HashSet<Order>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Order order1 = new Order();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.orderID for field OrderID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderID")) {
						if(nestedRow.getAs("OrderID")==null)
							order1.setOrderID(null);
						else{
							order1.setOrderID(Util.getIntegerValue(nestedRow.getAs("OrderID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.freight for field Freight			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Freight")) {
						if(nestedRow.getAs("Freight")==null)
							order1.setFreight(null);
						else{
							order1.setFreight(Util.getDoubleValue(nestedRow.getAs("Freight")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderDate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							order1.setOrderDate(null);
						else{
							order1.setOrderDate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.requiredDate for field RequiredDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("RequiredDate")) {
						if(nestedRow.getAs("RequiredDate")==null)
							order1.setRequiredDate(null);
						else{
							order1.setRequiredDate(Util.getLocalDateValue(nestedRow.getAs("RequiredDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipAddress for field ShipAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipAddress")) {
						if(nestedRow.getAs("ShipAddress")==null)
							order1.setShipAddress(null);
						else{
							order1.setShipAddress(Util.getStringValue(nestedRow.getAs("ShipAddress")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCity for field ShipCity			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCity")) {
						if(nestedRow.getAs("ShipCity")==null)
							order1.setShipCity(null);
						else{
							order1.setShipCity(Util.getStringValue(nestedRow.getAs("ShipCity")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipCountry for field ShipCountry			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipCountry")) {
						if(nestedRow.getAs("ShipCountry")==null)
							order1.setShipCountry(null);
						else{
							order1.setShipCountry(Util.getStringValue(nestedRow.getAs("ShipCountry")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipName for field ShipName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipName")) {
						if(nestedRow.getAs("ShipName")==null)
							order1.setShipName(null);
						else{
							order1.setShipName(Util.getStringValue(nestedRow.getAs("ShipName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipPostalCode for field ShipPostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipPostalCode")) {
						if(nestedRow.getAs("ShipPostalCode")==null)
							order1.setShipPostalCode(null);
						else{
							order1.setShipPostalCode(Util.getStringValue(nestedRow.getAs("ShipPostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shipRegion for field ShipRegion			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShipRegion")) {
						if(nestedRow.getAs("ShipRegion")==null)
							order1.setShipRegion(null);
						else{
							order1.setShipRegion(Util.getStringValue(nestedRow.getAs("ShipRegion")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.shippedDate for field ShippedDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("ShippedDate")) {
						if(nestedRow.getAs("ShippedDate")==null)
							order1.setShippedDate(null);
						else{
							order1.setShippedDate(Util.getLocalDateValue(nestedRow.getAs("ShippedDate")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(order1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Order.class));
		res= res.dropDuplicates(new String[]{"orderID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Order> getOrderRefListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderRef_condition,conditions.Condition<conditions.ProductAttribute> productRef_condition, conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition)		{
		MutableBoolean orderRef_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean productRef_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Composed_of> res_composed_of_orderRef;
		Dataset<Order> res_Order;
		// Role 'productRef' mapped to EmbeddedObject 'products' 'Order' containing 'Product' 
		productRef_refilter = new MutableBoolean(false);
		res_composed_of_orderRef = composed_ofService.getComposed_ofListInmyMongoDBOrdersproducts(productRef_condition, orderRef_condition, productRef_refilter, orderRef_refilter);
		if(productRef_refilter.booleanValue()) {
			if(all == null)
				all = new ProductServiceImpl().getProductList(productRef_condition);
			joinCondition = null;
			joinCondition = res_composed_of_orderRef.col("productRef.productID").equalTo(all.col("productID"));
			if(joinCondition == null)
				res_Order = res_composed_of_orderRef.join(all).select("orderRef.*").as(Encoders.bean(Order.class));
			else
				res_Order = res_composed_of_orderRef.join(all, joinCondition).select("orderRef.*").as(Encoders.bean(Order.class));
		
		} else
			res_Order = res_composed_of_orderRef.map((MapFunction<Composed_of,Order>) r -> r.getOrderRef(), Encoders.bean(Order.class));
		res_Order = res_Order.dropDuplicates(new String[] {"orderID"});
		datasetsPOJO.add(res_Order);
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(orderRef_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> orderRef_condition == null || orderRef_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getBoughtOrderListInBuys(conditions.Condition<conditions.OrderAttribute> boughtOrder_condition,conditions.Condition<conditions.CustomerAttribute> customerRef_condition)		{
		MutableBoolean boughtOrder_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Customer> all = null;
		boolean all_already_persisted = false;
		MutableBoolean customerRef_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Buys> res_buys_boughtOrder;
		Dataset<Order> res_Order;
		// Role 'boughtOrder' mapped to EmbeddedObject 'customer' - 'Customer' containing 'Order'
		customerRef_refilter = new MutableBoolean(false);
		res_buys_boughtOrder = buysService.getBuysListInmyMongoDBOrderscustomer(boughtOrder_condition, customerRef_condition, boughtOrder_refilter, customerRef_refilter);
		if(customerRef_refilter.booleanValue()) {
			if(all == null)
				all = new CustomerServiceImpl().getCustomerList(customerRef_condition);
			joinCondition = null;
			joinCondition = res_buys_boughtOrder.col("customerRef.iD").equalTo(all.col("iD"));
			if(joinCondition == null)
				res_Order = res_buys_boughtOrder.join(all).select("boughtOrder.*").as(Encoders.bean(Order.class));
			else
				res_Order = res_buys_boughtOrder.join(all, joinCondition).select("boughtOrder.*").as(Encoders.bean(Order.class));
		
		} else
			res_Order = res_buys_boughtOrder.map((MapFunction<Buys,Order>) r -> r.getBoughtOrder(), Encoders.bean(Order.class));
		res_Order = res_Order.dropDuplicates(new String[] {"orderID"});
		datasetsPOJO.add(res_Order);
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(boughtOrder_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> boughtOrder_condition == null || boughtOrder_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInHandles(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Employee> all = null;
		boolean all_already_persisted = false;
		MutableBoolean employeeRef_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'order' in reference 'orderHandler'. A->B Scenario
		employeeRef_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOorderHandlerorder = handlesService.getOrderTDOListOrderInOrderHandlerInOrdersFromMyMongoDB(order_condition, order_refilter);
		Dataset<EmployeeTDO> employeeTDOorderHandleremployeeRef = handlesService.getEmployeeTDOListEmployeeRefInOrderHandlerInOrdersFromMyMongoDB(employeeRef_condition, employeeRef_refilter);
		if(employeeRef_refilter.booleanValue()) {
			if(all == null)
				all = new EmployeeServiceImpl().getEmployeeList(employeeRef_condition);
			joinCondition = null;
			joinCondition = employeeTDOorderHandleremployeeRef.col("employeeID").equalTo(all.col("employeeID"));
			if(joinCondition == null)
				employeeTDOorderHandleremployeeRef = employeeTDOorderHandleremployeeRef.as("A").join(all).select("A.*").as(Encoders.bean(EmployeeTDO.class));
			else
				employeeTDOorderHandleremployeeRef = employeeTDOorderHandleremployeeRef.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(EmployeeTDO.class));
		}
	
		
		Dataset<Row> res_orderHandler = orderTDOorderHandlerorder.join(employeeTDOorderHandleremployeeRef
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
		Dataset<Order> res_Order_orderHandler = res_orderHandler.select( "orderID", "freight", "orderDate", "requiredDate", "shipAddress", "shipCity", "shipCountry", "shipName", "shipPostalCode", "shipRegion", "shippedDate", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_orderHandler = res_Order_orderHandler.dropDuplicates(new String[] {"orderID"});
		datasetsPOJO.add(res_Order_orderHandler);
		
		
		Dataset<Handles> res_handles_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertOrder(
		Order order,
		 List<Product> productRefComposed_of,
		Customer	customerRefBuys,
		Employee	employeeRefHandles,
		Double composed_of_unitPrice
	,
		Integer composed_of_quantity
	,
		Double composed_of_discount
	){
		boolean inserted = false;
		// Insert in standalone structures
		// Insert in structures containing double embedded role
		// Insert in descending structures
		inserted = insertOrderInOrdersFromMyMongoDB(order,productRefComposed_of,customerRefBuys,employeeRefHandles, composed_of_unitPrice, composed_of_quantity, composed_of_discount)|| inserted ;
		// Insert in ascending structures 
		// Insert in ref structures 
		// Insert in ref structures mapped to opposite role of mandatory role  
		return inserted;
	}
	
	public boolean insertOrderInOrdersFromMyMongoDB(Order order,
		 List<Product> productRefComposed_of,
		Customer	customerRefBuys,
		Employee	employeeRefHandles,
		Double composed_of_unitPrice
	,
		Integer composed_of_quantity
	,
		Double composed_of_discount
	)	{
			 // Implement Insert in descending complex struct
			Bson filter = new Document();
			Bson updateOp;
			Document docOrders_1 = new Document();
			docOrders_1.append("OrderID",order.getOrderID());
			// Ref 'orderHandler' mapped to mandatory role 'order'
			docOrders_1.append("EmployeeRef",employeeRefHandles.getEmployeeID());
			docOrders_1.append("Freight",order.getFreight());
			docOrders_1.append("OrderDate",order.getOrderDate());
			docOrders_1.append("RequiredDate",order.getRequiredDate());
			docOrders_1.append("ShipAddress",order.getShipAddress());
			docOrders_1.append("ShipCity",order.getShipCity());
			docOrders_1.append("ShipCountry",order.getShipCountry());
			docOrders_1.append("ShipName",order.getShipName());
			docOrders_1.append("ShipPostalCode",order.getShipPostalCode());
			docOrders_1.append("ShipRegion",order.getShipRegion());
			docOrders_1.append("ShippedDate",order.getShippedDate());
			// field 'customer' is mapped to mandatory role 'boughtOrder' with opposite role of type 'Customer'
					Customer customer = customerRefBuys;
					Document doccustomer_2 = new Document();
					doccustomer_2.append("CustomerID",customer.getID());
					doccustomer_2.append("ContactName",customer.getContactName());
					
					docOrders_1.append("customer", doccustomer_2);
			
			filter = eq("OrderID",order.getOrderID());
			updateOp = setOnInsert(docOrders_1);
			DBConnectionMgr.upsertMany(filter, updateOp, "Orders", "myMongoDB");
			return true;
		}
	private boolean inUpdateMethod = false;
	private List<Row> allOrderIdList = null;
	public void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set){
		inUpdateMethod = true;
		try {
	
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	
	
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void updateOrderRefListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderRefListInComposed_ofByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderRefListInComposed_of(orderRef_condition, null, null, set);
	}
	public void updateOrderRefListInComposed_ofByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderRefListInComposed_of(null, productRef_condition, null, set);
	}
	
	public void updateOrderRefListInComposed_ofByProductRef(
		pojo.Product productRef,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderRefListInComposed_of(null, null, composed_of_condition, set);
	}
	public void updateBoughtOrderListInBuys(
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition,
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateBoughtOrderListInBuysByBoughtOrderCondition(
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateBoughtOrderListInBuys(boughtOrder_condition, null, set);
	}
	public void updateBoughtOrderListInBuysByCustomerRefCondition(
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateBoughtOrderListInBuys(null, customerRef_condition, set);
	}
	
	public void updateBoughtOrderListInBuysByCustomerRef(
		pojo.Customer customerRef,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInHandles(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInHandlesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInHandles(order_condition, null, set);
	}
	public void updateOrderListInHandlesByEmployeeRefCondition(
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInHandles(null, employeeRef_condition, set);
	}
	
	public void updateOrderListInHandlesByEmployeeRef(
		pojo.Employee employeeRef,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		//TODO
	}
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void deleteOrderRefListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,	
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of){
			//TODO
		}
	
	public void deleteOrderRefListInComposed_ofByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition
	){
		deleteOrderRefListInComposed_of(orderRef_condition, null, null);
	}
	public void deleteOrderRefListInComposed_ofByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition
	){
		deleteOrderRefListInComposed_of(null, productRef_condition, null);
	}
	
	public void deleteOrderRefListInComposed_ofByProductRef(
		pojo.Product productRef 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		deleteOrderRefListInComposed_of(null, null, composed_of_condition);
	}
	public void deleteBoughtOrderListInBuys(	
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition,	
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition){
			//TODO
		}
	
	public void deleteBoughtOrderListInBuysByBoughtOrderCondition(
		conditions.Condition<conditions.OrderAttribute> boughtOrder_condition
	){
		deleteBoughtOrderListInBuys(boughtOrder_condition, null);
	}
	public void deleteBoughtOrderListInBuysByCustomerRefCondition(
		conditions.Condition<conditions.CustomerAttribute> customerRef_condition
	){
		deleteBoughtOrderListInBuys(null, customerRef_condition);
	}
	
	public void deleteBoughtOrderListInBuysByCustomerRef(
		pojo.Customer customerRef 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInHandles(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition){
			//TODO
		}
	
	public void deleteOrderListInHandlesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInHandles(order_condition, null);
	}
	public void deleteOrderListInHandlesByEmployeeRefCondition(
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition
	){
		deleteOrderListInHandles(null, employeeRef_condition);
	}
	
	public void deleteOrderListInHandlesByEmployeeRef(
		pojo.Employee employeeRef 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
