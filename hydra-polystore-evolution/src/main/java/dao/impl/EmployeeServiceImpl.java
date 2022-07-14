package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Employee;
import conditions.*;
import dao.services.EmployeeService;
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


public class EmployeeServiceImpl extends EmployeeService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeeServiceImpl.class);
	
	
	
	
	
	
	public static Pair<List<String>, List<String>> getBSONUpdateQueryInEmployeesFromMyMongoDB(conditions.SetClause<EmployeeAttribute> set) {
		List<String> res = new ArrayList<String>();
		Set<String> arrayFields = new HashSet<String>();
		if(set != null) {
			java.util.Map<String, java.util.Map<String, String>> longFieldValues = new java.util.HashMap<String, java.util.Map<String, String>>();
			java.util.Map<EmployeeAttribute, Object> clause = set.getClause();
			for(java.util.Map.Entry<EmployeeAttribute, Object> e : clause.entrySet()) {
				EmployeeAttribute attr = e.getKey();
				Object value = e.getValue();
				if(attr == EmployeeAttribute.employeeID ) {
					String fieldName = "EmployeeID";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.address ) {
					String fieldName = "Address";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.birthDate ) {
					String fieldName = "BirthDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.city ) {
					String fieldName = "City";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.country ) {
					String fieldName = "Country";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.extension ) {
					String fieldName = "Extension";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.firstName ) {
					String fieldName = "FirstName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.hireDate ) {
					String fieldName = "HireDate";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.homePhone ) {
					String fieldName = "HomePhone";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.lastName ) {
					String fieldName = "LastName";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.notes ) {
					String fieldName = "Notes";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.photo ) {
					String fieldName = "Photo";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.photoPath ) {
					String fieldName = "PhotoPath";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.postalCode ) {
					String fieldName = "PostalCode";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.region ) {
					String fieldName = "Region";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.salary ) {
					String fieldName = "Salary";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.title ) {
					String fieldName = "Title";
					fieldName = "'" + fieldName + "'";
					res.add(fieldName + " : " + Util.getDelimitedMongoValue((value == null ? null : value.getClass()), (value == null ? null : value.toString())));
				}
				if(attr == EmployeeAttribute.titleOfCourtesy ) {
					String fieldName = "TitleOfCourtesy";
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
	
	public static String getBSONMatchQueryInEmployeesFromMyMongoDB(Condition<EmployeeAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				EmployeeAttribute attr = ((SimpleCondition<EmployeeAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<EmployeeAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<EmployeeAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == EmployeeAttribute.employeeID ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "EmployeeID': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.address ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Address': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.birthDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "BirthDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.city ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "City': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.country ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Country': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.extension ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Extension': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.firstName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "FirstName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.hireDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "HireDate': {" + mongoOp + ": " +  "ISODate("+preparedValue + ")}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.homePhone ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "HomePhone': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.lastName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "LastName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.notes ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Notes': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.photo ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Photo': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.photoPath ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "PhotoPath': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.postalCode ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "PostalCode': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.region ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Region': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.salary ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Salary': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.title ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "Title': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == EmployeeAttribute.titleOfCourtesy ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "TitleOfCourtesy': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInEmployeesFromMyMongoDB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInEmployeesFromMyMongoDB(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public static Pair<String, List<String>> getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(Condition<EmployeeAttribute> condition, final List<String> arrayVariableNames, Set<String> arrayVariablesUsed, MutableBoolean refilterFlag) {	
		String query = null;
		List<String> arrayFilters = new ArrayList<String>();
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				String bson = null;
				EmployeeAttribute attr = ((SimpleCondition<EmployeeAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<EmployeeAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<EmployeeAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == EmployeeAttribute.employeeID ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "EmployeeID': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.address ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Address': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.birthDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "BirthDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.city ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "City': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.country ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Country': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.extension ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Extension': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.firstName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "FirstName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.hireDate ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "HireDate': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.homePhone ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "HomePhone': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.lastName ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "LastName': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.notes ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Notes': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.photo ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Photo': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.photoPath ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "PhotoPath': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.postalCode ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "PostalCode': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.region ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Region': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.salary ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Salary': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.title ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "Title': {" + mongoOp + ": " + preparedValue + "}";
					
						boolean arrayVar = false;
	
						bson = "'" + bson;
						if(arrayVar)
							arrayFilters.add(bson);
						else
							query = bson;
					}
					if(attr == EmployeeAttribute.titleOfCourtesy ) {
						isConditionAttrEncountered = true;
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						bson = "TitleOfCourtesy': {" + mongoOp + ": " + preparedValue + "}";
					
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
				Pair<String, List<String>> bsonLeft = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getLeftCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);
				Pair<String, List<String>> bsonRight = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(((AndCondition)condition).getRightCondition(), arrayVariableNames, arrayVariablesUsed, refilterFlag);			
				
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
	
	
	
	public Dataset<Employee> getEmployeeListInEmployeesFromMyMongoDB(conditions.Condition<conditions.EmployeeAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = EmployeeServiceImpl.getBSONMatchQueryInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getDatasetFromMongoDB("myMongoDB", "Employees", bsonQuery);
	
		Dataset<Employee> res = dataset.flatMap((FlatMapFunction<Row, Employee>) r -> {
				Set<Employee> list_res = new HashSet<Employee>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Employee employee1 = new Employee();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Employee.employeeID for field EmployeeID			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("EmployeeID")) {
						if(nestedRow.getAs("EmployeeID")==null)
							employee1.setEmployeeID(null);
						else{
							employee1.setEmployeeID(Util.getIntegerValue(nestedRow.getAs("EmployeeID")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.address for field Address			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Address")) {
						if(nestedRow.getAs("Address")==null)
							employee1.setAddress(null);
						else{
							employee1.setAddress(Util.getStringValue(nestedRow.getAs("Address")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.birthDate for field BirthDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("BirthDate")) {
						if(nestedRow.getAs("BirthDate")==null)
							employee1.setBirthDate(null);
						else{
							employee1.setBirthDate(Util.getLocalDateValue(nestedRow.getAs("BirthDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.city for field City			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("City")) {
						if(nestedRow.getAs("City")==null)
							employee1.setCity(null);
						else{
							employee1.setCity(Util.getStringValue(nestedRow.getAs("City")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.country for field Country			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Country")) {
						if(nestedRow.getAs("Country")==null)
							employee1.setCountry(null);
						else{
							employee1.setCountry(Util.getStringValue(nestedRow.getAs("Country")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.extension for field Extension			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Extension")) {
						if(nestedRow.getAs("Extension")==null)
							employee1.setExtension(null);
						else{
							employee1.setExtension(Util.getStringValue(nestedRow.getAs("Extension")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.firstName for field FirstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("FirstName")) {
						if(nestedRow.getAs("FirstName")==null)
							employee1.setFirstName(null);
						else{
							employee1.setFirstName(Util.getStringValue(nestedRow.getAs("FirstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.hireDate for field HireDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HireDate")) {
						if(nestedRow.getAs("HireDate")==null)
							employee1.setHireDate(null);
						else{
							employee1.setHireDate(Util.getLocalDateValue(nestedRow.getAs("HireDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.homePhone for field HomePhone			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("HomePhone")) {
						if(nestedRow.getAs("HomePhone")==null)
							employee1.setHomePhone(null);
						else{
							employee1.setHomePhone(Util.getStringValue(nestedRow.getAs("HomePhone")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.lastName for field LastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("LastName")) {
						if(nestedRow.getAs("LastName")==null)
							employee1.setLastName(null);
						else{
							employee1.setLastName(Util.getStringValue(nestedRow.getAs("LastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.notes for field Notes			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Notes")) {
						if(nestedRow.getAs("Notes")==null)
							employee1.setNotes(null);
						else{
							employee1.setNotes(Util.getStringValue(nestedRow.getAs("Notes")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photo for field Photo			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Photo")) {
						if(nestedRow.getAs("Photo")==null)
							employee1.setPhoto(null);
						else{
							employee1.setPhoto(Util.getByteArrayValue(nestedRow.getAs("Photo")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.photoPath for field PhotoPath			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PhotoPath")) {
						if(nestedRow.getAs("PhotoPath")==null)
							employee1.setPhotoPath(null);
						else{
							employee1.setPhotoPath(Util.getStringValue(nestedRow.getAs("PhotoPath")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.postalCode for field PostalCode			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("PostalCode")) {
						if(nestedRow.getAs("PostalCode")==null)
							employee1.setPostalCode(null);
						else{
							employee1.setPostalCode(Util.getStringValue(nestedRow.getAs("PostalCode")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.region for field Region			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Region")) {
						if(nestedRow.getAs("Region")==null)
							employee1.setRegion(null);
						else{
							employee1.setRegion(Util.getStringValue(nestedRow.getAs("Region")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.salary for field Salary			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Salary")) {
						if(nestedRow.getAs("Salary")==null)
							employee1.setSalary(null);
						else{
							employee1.setSalary(Util.getDoubleValue(nestedRow.getAs("Salary")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.title for field Title			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("Title")) {
						if(nestedRow.getAs("Title")==null)
							employee1.setTitle(null);
						else{
							employee1.setTitle(Util.getStringValue(nestedRow.getAs("Title")));
							toAdd1 = true;					
							}
					}
					// 	attribute Employee.titleOfCourtesy for field TitleOfCourtesy			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TitleOfCourtesy")) {
						if(nestedRow.getAs("TitleOfCourtesy")==null)
							employee1.setTitleOfCourtesy(null);
						else{
							employee1.setTitleOfCourtesy(Util.getStringValue(nestedRow.getAs("TitleOfCourtesy")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(employee1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Employee.class));
		res= res.dropDuplicates(new String[]{"employeeID"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Employee> getEmployeeRefListInHandles(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition)		{
		MutableBoolean employeeRef_refilter = new MutableBoolean(false);
		List<Dataset<Employee>> datasetsPOJO = new ArrayList<Dataset<Employee>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'orderHandler'  B->A Scenario
		Dataset<OrderTDO> orderTDOorderHandlerorder = handlesService.getOrderTDOListOrderInOrderHandlerInOrdersFromMyMongoDB(order_condition, order_refilter);
		Dataset<EmployeeTDO> employeeTDOorderHandleremployeeRef = handlesService.getEmployeeTDOListEmployeeRefInOrderHandlerInOrdersFromMyMongoDB(employeeRef_condition, employeeRef_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(order_condition);
			joinCondition = null;
			joinCondition = orderTDOorderHandlerorder.col("orderID").equalTo(all.col("orderID"));
			if(joinCondition == null)
				orderTDOorderHandlerorder = orderTDOorderHandlerorder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDOorderHandlerorder = orderTDOorderHandlerorder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_orderHandler = 
			employeeTDOorderHandleremployeeRef.join(orderTDOorderHandlerorder
				.withColumnRenamed("orderID", "Order_orderID")
				.withColumnRenamed("freight", "Order_freight")
				.withColumnRenamed("orderDate", "Order_orderDate")
				.withColumnRenamed("requiredDate", "Order_requiredDate")
				.withColumnRenamed("shipAddress", "Order_shipAddress")
				.withColumnRenamed("shipCity", "Order_shipCity")
				.withColumnRenamed("shipCountry", "Order_shipCountry")
				.withColumnRenamed("shipName", "Order_shipName")
				.withColumnRenamed("shipPostalCode", "Order_shipPostalCode")
				.withColumnRenamed("shipRegion", "Order_shipRegion")
				.withColumnRenamed("shippedDate", "Order_shippedDate")
				.withColumnRenamed("logEvents", "Order_logEvents"),
				employeeTDOorderHandleremployeeRef.col("myMongoDB_Orders_orderHandler_target_EmployeeID").equalTo(orderTDOorderHandlerorder.col("myMongoDB_Orders_orderHandler_source_EmployeeRef")));
		Dataset<Employee> res_Employee_orderHandler = res_orderHandler.select( "employeeID", "address", "birthDate", "city", "country", "extension", "firstName", "hireDate", "homePhone", "lastName", "notes", "photo", "photoPath", "postalCode", "region", "salary", "title", "titleOfCourtesy", "logEvents").as(Encoders.bean(Employee.class));
		res_Employee_orderHandler = res_Employee_orderHandler.dropDuplicates(new String[] {"employeeID"});
		datasetsPOJO.add(res_Employee_orderHandler);
		
		Dataset<Handles> res_handles_employeeRef;
		Dataset<Employee> res_Employee;
		
		
		//Join datasets or return 
		Dataset<Employee> res = fullOuterJoinsEmployee(datasetsPOJO);
		if(res == null)
			return null;
	
		if(employeeRef_refilter.booleanValue())
			res = res.filter((FilterFunction<Employee>) r -> employeeRef_condition == null || employeeRef_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertEmployee(Employee employee){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertEmployeeInEmployeesFromMyMongoDB(employee) || inserted ;
		return inserted;
	}
	
	public boolean insertEmployeeInEmployeesFromMyMongoDB(Employee employee)	{
		String idvalue="";
		idvalue+=employee.getEmployeeID();
		boolean entityExists = false; // Modify in acceleo code (in 'main.services.insert.entitytype.generateSimpleInsertMethods.mtl') to generate checking before insert
		if(!entityExists){
		Bson filter = new Document();
		Bson updateOp;
		Document docEmployees_1 = new Document();
		docEmployees_1.append("EmployeeID",employee.getEmployeeID());
		docEmployees_1.append("Address",employee.getAddress());
		docEmployees_1.append("BirthDate",employee.getBirthDate());
		docEmployees_1.append("City",employee.getCity());
		docEmployees_1.append("Country",employee.getCountry());
		docEmployees_1.append("Extension",employee.getExtension());
		docEmployees_1.append("FirstName",employee.getFirstName());
		docEmployees_1.append("HireDate",employee.getHireDate());
		docEmployees_1.append("HomePhone",employee.getHomePhone());
		docEmployees_1.append("LastName",employee.getLastName());
		docEmployees_1.append("Notes",employee.getNotes());
		docEmployees_1.append("Photo",employee.getPhoto());
		docEmployees_1.append("PhotoPath",employee.getPhotoPath());
		docEmployees_1.append("PostalCode",employee.getPostalCode());
		docEmployees_1.append("Region",employee.getRegion());
		docEmployees_1.append("Salary",employee.getSalary());
		docEmployees_1.append("Title",employee.getTitle());
		docEmployees_1.append("TitleOfCourtesy",employee.getTitleOfCourtesy());
		
		filter = eq("EmployeeID",employee.getEmployeeID());
		updateOp = setOnInsert(docEmployees_1);
		DBConnectionMgr.upsertMany(filter, updateOp, "Employees", "myMongoDB");
			logger.info("Inserted [Employee] entity ID [{}] in [Employees] in database [MyMongoDB]", idvalue);
		}
		else
			logger.warn("[Employee] entity ID [{}] already present in [Employees] in database [MyMongoDB]", idvalue);
		return !entityExists;
	} 
	
	private boolean inUpdateMethod = false;
	private List<Row> allEmployeeIdList = null;
	public void updateEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition, conditions.SetClause<conditions.EmployeeAttribute> set){
		inUpdateMethod = true;
		try {
			MutableBoolean refilterInEmployeesFromMyMongoDB = new MutableBoolean(false);
			getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(condition, new ArrayList<String>(), new HashSet<String>(), refilterInEmployeesFromMyMongoDB);
			// one first updates in the structures necessitating to execute a "SELECT *" query to establish the update condition 
			if(refilterInEmployeesFromMyMongoDB.booleanValue())
				updateEmployeeListInEmployeesFromMyMongoDB(condition, set);
		
	
			if(!refilterInEmployeesFromMyMongoDB.booleanValue())
				updateEmployeeListInEmployeesFromMyMongoDB(condition, set);
	
		} finally {
			inUpdateMethod = false;
		}
	}
	
	
	public void updateEmployeeListInEmployeesFromMyMongoDB(Condition<EmployeeAttribute> condition, SetClause<EmployeeAttribute> set) {
		Pair<List<String>, List<String>> updates = getBSONUpdateQueryInEmployeesFromMyMongoDB(set);
		List<String> sets = updates.getLeft();
		final List<String> arrayVariableNames = updates.getRight();
		String setBSON = null;
		for(int i = 0; i < sets.size(); i++) {
			if(i == 0)
				setBSON = sets.get(i);
			else
				setBSON += ", " + sets.get(i);
		}
		
		if(setBSON == null)
			return;
		
		Document updateQuery = null;
		setBSON = "{$set: {" + setBSON + "}}";
		updateQuery = Document.parse(setBSON);
		
		MutableBoolean refilter = new MutableBoolean(false);
		Set<String> arrayVariablesUsed = new HashSet<String>();
		Pair<String, List<String>> queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(condition, arrayVariableNames, arrayVariablesUsed, refilter);
		Document query = null;
		String bsonQuery = queryAndArrayFilter.getLeft();
		if(bsonQuery != null) {
			bsonQuery = "{" + bsonQuery + "}";
			query = Document.parse(bsonQuery);	
		}
		
		List<Bson> arrayFilterDocs = new ArrayList<Bson>();
		List<String> arrayFilters = queryAndArrayFilter.getRight();
		for(String arrayFilter : arrayFilters)
			arrayFilterDocs.add(Document.parse( "{" + arrayFilter + "}"));
		
		for(String arrayVariableName : arrayVariableNames)
			if(!arrayVariablesUsed.contains(arrayVariableName)) {
				arrayFilterDocs.add(Document.parse("{" + arrayVariableName + ": {$exists: true}}"));
			}
		
		
		if(!refilter.booleanValue()) {
			if(arrayFilterDocs.size() == 0) {
				DBConnectionMgr.update(query, updateQuery, "Employees", "myMongoDB");
			} else {
				DBConnectionMgr.upsertMany(query, updateQuery, arrayFilterDocs, "Employees", "myMongoDB");
			}
		
			
		} else {
			if(!inUpdateMethod || allEmployeeIdList == null)
				allEmployeeIdList = this.getEmployeeList(condition).select("employeeID").collectAsList();
			List<com.mongodb.client.model.UpdateManyModel<Document>> updateQueries = new ArrayList<com.mongodb.client.model.UpdateManyModel<Document>>();
			for(Row row : allEmployeeIdList) {
				Condition<EmployeeAttribute> conditionId = null;
				conditionId = Condition.simple(EmployeeAttribute.employeeID, Operator.EQUALS, row.getAs("employeeID"));
		
				arrayVariablesUsed = new HashSet<String>();
				queryAndArrayFilter = getBSONQueryAndArrayFilterForUpdateQueryInEmployeesFromMyMongoDB(conditionId, arrayVariableNames, arrayVariablesUsed, refilter);
				query = null;
				bsonQuery = queryAndArrayFilter.getLeft();
				if(bsonQuery != null) {
					bsonQuery = "{" + bsonQuery + "}";
					query = Document.parse(bsonQuery);	
				}
				
				arrayFilterDocs = new ArrayList<Bson>();
				arrayFilters = queryAndArrayFilter.getRight();
				for(String arrayFilter : arrayFilters)
					arrayFilterDocs.add(Document.parse( "{" + arrayFilter + "}"));
				
				for(String arrayVariableName : arrayVariableNames)
					if(!arrayVariablesUsed.contains(arrayVariableName)) {
						arrayFilterDocs.add(Document.parse("{" + arrayVariableName + ": {$exists: true}}"));
					}
				if(arrayFilterDocs.size() == 0)
					updateQueries.add(new com.mongodb.client.model.UpdateManyModel<Document>(query, updateQuery));
				else
					updateQueries.add(new com.mongodb.client.model.UpdateManyModel<Document>(query, updateQuery, new com.mongodb.client.model.UpdateOptions().arrayFilters(arrayFilterDocs)));
			}
		
			DBConnectionMgr.bulkUpdatesInMongoDB(updateQueries, "Employees", "myMongoDB");
		}
	}
	
	
	
	public void updateEmployee(pojo.Employee employee) {
		//TODO using the id
		return;
	}
	public void updateEmployeeRefListInHandles(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition,
		
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		//TODO
	}
	
	public void updateEmployeeRefListInHandlesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateEmployeeRefListInHandles(order_condition, null, set);
	}
	
	public void updateEmployeeRefInHandlesByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.EmployeeAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateEmployeeRefListInHandlesByEmployeeRefCondition(
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition,
		conditions.SetClause<conditions.EmployeeAttribute> set
	){
		updateEmployeeRefListInHandles(null, employeeRef_condition, set);
	}
	
	
	public void deleteEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition){
		//TODO
	}
	
	public void deleteEmployee(pojo.Employee employee) {
		//TODO using the id
		return;
	}
	public void deleteEmployeeRefListInHandles(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition){
			//TODO
		}
	
	public void deleteEmployeeRefListInHandlesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteEmployeeRefListInHandles(order_condition, null);
	}
	
	public void deleteEmployeeRefInHandlesByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteEmployeeRefListInHandlesByEmployeeRefCondition(
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition
	){
		deleteEmployeeRefListInHandles(null, employeeRef_condition);
	}
	
}
