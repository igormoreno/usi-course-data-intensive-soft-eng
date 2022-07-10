package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Employee;
import conditions.EmployeeAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang3.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.*;
import conditions.EmployeeAttribute;
import pojo.Handles;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class EmployeeService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EmployeeService.class);
	protected HandlesService handlesService = new dao.impl.HandlesServiceImpl();
	


	public static enum ROLE_NAME {
		HANDLES_EMPLOYEEREF
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.HANDLES_EMPLOYEEREF, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public EmployeeService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public EmployeeService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Employee> getEmployeeList(){
		return getEmployeeList(null);
	}
	
	public Dataset<Employee> getEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition){
		StopWatch stopwatch = new StopWatch();
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Employee>> datasets = new ArrayList<Dataset<Employee>>();
		Dataset<Employee> d = null;
		d = getEmployeeListInEmployeesFromMyMongoDB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsEmployee(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Employee>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"employeeID"});
		logger.info("Execution time in seconds : ", stopwatch.getElapsedTimeInSeconds());
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Employee> getEmployeeListInEmployeesFromMyMongoDB(conditions.Condition<conditions.EmployeeAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Employee getEmployeeById(Integer employeeID){
		Condition cond;
		cond = Condition.simple(EmployeeAttribute.employeeID, conditions.Operator.EQUALS, employeeID);
		Dataset<Employee> res = getEmployeeList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Employee> getEmployeeListByEmployeeID(Integer employeeID) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.employeeID, conditions.Operator.EQUALS, employeeID));
	}
	
	public Dataset<Employee> getEmployeeListByAddress(String address) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	public Dataset<Employee> getEmployeeListByBirthDate(LocalDate birthDate) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.birthDate, conditions.Operator.EQUALS, birthDate));
	}
	
	public Dataset<Employee> getEmployeeListByCity(String city) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	public Dataset<Employee> getEmployeeListByCountry(String country) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.country, conditions.Operator.EQUALS, country));
	}
	
	public Dataset<Employee> getEmployeeListByExtension(String extension) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.extension, conditions.Operator.EQUALS, extension));
	}
	
	public Dataset<Employee> getEmployeeListByFirstName(String firstName) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.firstName, conditions.Operator.EQUALS, firstName));
	}
	
	public Dataset<Employee> getEmployeeListByHireDate(LocalDate hireDate) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.hireDate, conditions.Operator.EQUALS, hireDate));
	}
	
	public Dataset<Employee> getEmployeeListByHomePhone(String homePhone) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.homePhone, conditions.Operator.EQUALS, homePhone));
	}
	
	public Dataset<Employee> getEmployeeListByLastName(String lastName) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.lastName, conditions.Operator.EQUALS, lastName));
	}
	
	public Dataset<Employee> getEmployeeListByNotes(String notes) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.notes, conditions.Operator.EQUALS, notes));
	}
	
	public Dataset<Employee> getEmployeeListByPhoto(byte[] photo) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.photo, conditions.Operator.EQUALS, photo));
	}
	
	public Dataset<Employee> getEmployeeListByPhotoPath(String photoPath) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.photoPath, conditions.Operator.EQUALS, photoPath));
	}
	
	public Dataset<Employee> getEmployeeListByPostalCode(String postalCode) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.postalCode, conditions.Operator.EQUALS, postalCode));
	}
	
	public Dataset<Employee> getEmployeeListByRegion(String region) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.region, conditions.Operator.EQUALS, region));
	}
	
	public Dataset<Employee> getEmployeeListBySalary(Double salary) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.salary, conditions.Operator.EQUALS, salary));
	}
	
	public Dataset<Employee> getEmployeeListByTitle(String title) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.title, conditions.Operator.EQUALS, title));
	}
	
	public Dataset<Employee> getEmployeeListByTitleOfCourtesy(String titleOfCourtesy) {
		return getEmployeeList(conditions.Condition.simple(conditions.EmployeeAttribute.titleOfCourtesy, conditions.Operator.EQUALS, titleOfCourtesy));
	}
	
	
	
	public static Dataset<Employee> fullOuterJoinsEmployee(List<Dataset<Employee>> datasetsPOJO) {
		return fullOuterJoinsEmployee(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Employee> fullLeftOuterJoinsEmployee(List<Dataset<Employee>> datasetsPOJO) {
		return fullOuterJoinsEmployee(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Employee> fullOuterJoinsEmployee(List<Dataset<Employee>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Employee> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("employeeID");
			logger.debug("Start {} of [{}] datasets of [Employee] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("birthDate", "birthDate_1")
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("country", "country_1")
								.withColumnRenamed("extension", "extension_1")
								.withColumnRenamed("firstName", "firstName_1")
								.withColumnRenamed("hireDate", "hireDate_1")
								.withColumnRenamed("homePhone", "homePhone_1")
								.withColumnRenamed("lastName", "lastName_1")
								.withColumnRenamed("notes", "notes_1")
								.withColumnRenamed("photo", "photo_1")
								.withColumnRenamed("photoPath", "photoPath_1")
								.withColumnRenamed("postalCode", "postalCode_1")
								.withColumnRenamed("region", "region_1")
								.withColumnRenamed("salary", "salary_1")
								.withColumnRenamed("title", "title_1")
								.withColumnRenamed("titleOfCourtesy", "titleOfCourtesy_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("birthDate", "birthDate_" + i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("country", "country_" + i)
								.withColumnRenamed("extension", "extension_" + i)
								.withColumnRenamed("firstName", "firstName_" + i)
								.withColumnRenamed("hireDate", "hireDate_" + i)
								.withColumnRenamed("homePhone", "homePhone_" + i)
								.withColumnRenamed("lastName", "lastName_" + i)
								.withColumnRenamed("notes", "notes_" + i)
								.withColumnRenamed("photo", "photo_" + i)
								.withColumnRenamed("photoPath", "photoPath_" + i)
								.withColumnRenamed("postalCode", "postalCode_" + i)
								.withColumnRenamed("region", "region_" + i)
								.withColumnRenamed("salary", "salary_" + i)
								.withColumnRenamed("title", "title_" + i)
								.withColumnRenamed("titleOfCourtesy", "titleOfCourtesy_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Employee] objects"); 
			d = res.map((MapFunction<Row, Employee>) r -> {
					Employee employee_res = new Employee();
					
					// attribute 'Employee.employeeID'
					Integer firstNotNull_employeeID = Util.getIntegerValue(r.getAs("employeeID"));
					employee_res.setEmployeeID(firstNotNull_employeeID);
					
					// attribute 'Employee.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.address': " + firstNotNull_address + " and " + address2 + "." );
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					employee_res.setAddress(firstNotNull_address);
					
					// attribute 'Employee.birthDate'
					LocalDate firstNotNull_birthDate = Util.getLocalDateValue(r.getAs("birthDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate birthDate2 = Util.getLocalDateValue(r.getAs("birthDate_" + i));
						if (firstNotNull_birthDate != null && birthDate2 != null && !firstNotNull_birthDate.equals(birthDate2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_birthDate + " and " + birthDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.birthDate': " + firstNotNull_birthDate + " and " + birthDate2 + "." );
						}
						if (firstNotNull_birthDate == null && birthDate2 != null) {
							firstNotNull_birthDate = birthDate2;
						}
					}
					employee_res.setBirthDate(firstNotNull_birthDate);
					
					// attribute 'Employee.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.city': " + firstNotNull_city + " and " + city2 + "." );
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					employee_res.setCity(firstNotNull_city);
					
					// attribute 'Employee.country'
					String firstNotNull_country = Util.getStringValue(r.getAs("country"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String country2 = Util.getStringValue(r.getAs("country_" + i));
						if (firstNotNull_country != null && country2 != null && !firstNotNull_country.equals(country2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.country': " + firstNotNull_country + " and " + country2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.country': " + firstNotNull_country + " and " + country2 + "." );
						}
						if (firstNotNull_country == null && country2 != null) {
							firstNotNull_country = country2;
						}
					}
					employee_res.setCountry(firstNotNull_country);
					
					// attribute 'Employee.extension'
					String firstNotNull_extension = Util.getStringValue(r.getAs("extension"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String extension2 = Util.getStringValue(r.getAs("extension_" + i));
						if (firstNotNull_extension != null && extension2 != null && !firstNotNull_extension.equals(extension2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_extension + " and " + extension2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.extension': " + firstNotNull_extension + " and " + extension2 + "." );
						}
						if (firstNotNull_extension == null && extension2 != null) {
							firstNotNull_extension = extension2;
						}
					}
					employee_res.setExtension(firstNotNull_extension);
					
					// attribute 'Employee.firstName'
					String firstNotNull_firstName = Util.getStringValue(r.getAs("firstName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String firstName2 = Util.getStringValue(r.getAs("firstName_" + i));
						if (firstNotNull_firstName != null && firstName2 != null && !firstNotNull_firstName.equals(firstName2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
						}
						if (firstNotNull_firstName == null && firstName2 != null) {
							firstNotNull_firstName = firstName2;
						}
					}
					employee_res.setFirstName(firstNotNull_firstName);
					
					// attribute 'Employee.hireDate'
					LocalDate firstNotNull_hireDate = Util.getLocalDateValue(r.getAs("hireDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate hireDate2 = Util.getLocalDateValue(r.getAs("hireDate_" + i));
						if (firstNotNull_hireDate != null && hireDate2 != null && !firstNotNull_hireDate.equals(hireDate2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_hireDate + " and " + hireDate2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.hireDate': " + firstNotNull_hireDate + " and " + hireDate2 + "." );
						}
						if (firstNotNull_hireDate == null && hireDate2 != null) {
							firstNotNull_hireDate = hireDate2;
						}
					}
					employee_res.setHireDate(firstNotNull_hireDate);
					
					// attribute 'Employee.homePhone'
					String firstNotNull_homePhone = Util.getStringValue(r.getAs("homePhone"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String homePhone2 = Util.getStringValue(r.getAs("homePhone_" + i));
						if (firstNotNull_homePhone != null && homePhone2 != null && !firstNotNull_homePhone.equals(homePhone2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_homePhone + " and " + homePhone2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.homePhone': " + firstNotNull_homePhone + " and " + homePhone2 + "." );
						}
						if (firstNotNull_homePhone == null && homePhone2 != null) {
							firstNotNull_homePhone = homePhone2;
						}
					}
					employee_res.setHomePhone(firstNotNull_homePhone);
					
					// attribute 'Employee.lastName'
					String firstNotNull_lastName = Util.getStringValue(r.getAs("lastName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lastName2 = Util.getStringValue(r.getAs("lastName_" + i));
						if (firstNotNull_lastName != null && lastName2 != null && !firstNotNull_lastName.equals(lastName2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
						}
						if (firstNotNull_lastName == null && lastName2 != null) {
							firstNotNull_lastName = lastName2;
						}
					}
					employee_res.setLastName(firstNotNull_lastName);
					
					// attribute 'Employee.notes'
					String firstNotNull_notes = Util.getStringValue(r.getAs("notes"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String notes2 = Util.getStringValue(r.getAs("notes_" + i));
						if (firstNotNull_notes != null && notes2 != null && !firstNotNull_notes.equals(notes2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_notes + " and " + notes2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.notes': " + firstNotNull_notes + " and " + notes2 + "." );
						}
						if (firstNotNull_notes == null && notes2 != null) {
							firstNotNull_notes = notes2;
						}
					}
					employee_res.setNotes(firstNotNull_notes);
					
					// attribute 'Employee.photo'
					byte[] firstNotNull_photo = Util.getByteArrayValue(r.getAs("photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						byte[] photo2 = Util.getByteArrayValue(r.getAs("photo_" + i));
						if (firstNotNull_photo != null && photo2 != null && !firstNotNull_photo.equals(photo2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_photo + " and " + photo2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.photo': " + firstNotNull_photo + " and " + photo2 + "." );
						}
						if (firstNotNull_photo == null && photo2 != null) {
							firstNotNull_photo = photo2;
						}
					}
					employee_res.setPhoto(firstNotNull_photo);
					
					// attribute 'Employee.photoPath'
					String firstNotNull_photoPath = Util.getStringValue(r.getAs("photoPath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String photoPath2 = Util.getStringValue(r.getAs("photoPath_" + i));
						if (firstNotNull_photoPath != null && photoPath2 != null && !firstNotNull_photoPath.equals(photoPath2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_photoPath + " and " + photoPath2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.photoPath': " + firstNotNull_photoPath + " and " + photoPath2 + "." );
						}
						if (firstNotNull_photoPath == null && photoPath2 != null) {
							firstNotNull_photoPath = photoPath2;
						}
					}
					employee_res.setPhotoPath(firstNotNull_photoPath);
					
					// attribute 'Employee.postalCode'
					String firstNotNull_postalCode = Util.getStringValue(r.getAs("postalCode"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String postalCode2 = Util.getStringValue(r.getAs("postalCode_" + i));
						if (firstNotNull_postalCode != null && postalCode2 != null && !firstNotNull_postalCode.equals(postalCode2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.postalCode': " + firstNotNull_postalCode + " and " + postalCode2 + "." );
						}
						if (firstNotNull_postalCode == null && postalCode2 != null) {
							firstNotNull_postalCode = postalCode2;
						}
					}
					employee_res.setPostalCode(firstNotNull_postalCode);
					
					// attribute 'Employee.region'
					String firstNotNull_region = Util.getStringValue(r.getAs("region"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String region2 = Util.getStringValue(r.getAs("region_" + i));
						if (firstNotNull_region != null && region2 != null && !firstNotNull_region.equals(region2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.region': " + firstNotNull_region + " and " + region2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.region': " + firstNotNull_region + " and " + region2 + "." );
						}
						if (firstNotNull_region == null && region2 != null) {
							firstNotNull_region = region2;
						}
					}
					employee_res.setRegion(firstNotNull_region);
					
					// attribute 'Employee.salary'
					Double firstNotNull_salary = Util.getDoubleValue(r.getAs("salary"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double salary2 = Util.getDoubleValue(r.getAs("salary_" + i));
						if (firstNotNull_salary != null && salary2 != null && !firstNotNull_salary.equals(salary2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_salary + " and " + salary2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.salary': " + firstNotNull_salary + " and " + salary2 + "." );
						}
						if (firstNotNull_salary == null && salary2 != null) {
							firstNotNull_salary = salary2;
						}
					}
					employee_res.setSalary(firstNotNull_salary);
					
					// attribute 'Employee.title'
					String firstNotNull_title = Util.getStringValue(r.getAs("title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String title2 = Util.getStringValue(r.getAs("title_" + i));
						if (firstNotNull_title != null && title2 != null && !firstNotNull_title.equals(title2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.title': " + firstNotNull_title + " and " + title2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.title': " + firstNotNull_title + " and " + title2 + "." );
						}
						if (firstNotNull_title == null && title2 != null) {
							firstNotNull_title = title2;
						}
					}
					employee_res.setTitle(firstNotNull_title);
					
					// attribute 'Employee.titleOfCourtesy'
					String firstNotNull_titleOfCourtesy = Util.getStringValue(r.getAs("titleOfCourtesy"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String titleOfCourtesy2 = Util.getStringValue(r.getAs("titleOfCourtesy_" + i));
						if (firstNotNull_titleOfCourtesy != null && titleOfCourtesy2 != null && !firstNotNull_titleOfCourtesy.equals(titleOfCourtesy2)) {
							employee_res.addLogEvent("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_titleOfCourtesy + " and " + titleOfCourtesy2 + "." );
							logger.warn("Data consistency problem for [Employee - id :"+employee_res.getEmployeeID()+"]: different values found for attribute 'Employee.titleOfCourtesy': " + firstNotNull_titleOfCourtesy + " and " + titleOfCourtesy2 + "." );
						}
						if (firstNotNull_titleOfCourtesy == null && titleOfCourtesy2 != null) {
							firstNotNull_titleOfCourtesy = titleOfCourtesy2;
						}
					}
					employee_res.setTitleOfCourtesy(firstNotNull_titleOfCourtesy);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							employee_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							employee_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return employee_res;
				}, Encoders.bean(Employee.class));
			return d;
	}
	
	
	
	public Employee getEmployee(Employee.handles role, Order order) {
		if(role != null) {
			if(role.equals(Employee.handles.employeeRef))
				return getEmployeeRefInHandlesByOrder(order);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.handles role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Employee.handles.employeeRef))
				return getEmployeeRefListInHandlesByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Employee> getEmployeeList(Employee.handles role, Condition<OrderAttribute> condition1, Condition<EmployeeAttribute> condition2) {
		if(role != null) {
			if(role.equals(Employee.handles.employeeRef))
				return getEmployeeRefListInHandles(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Employee> getEmployeeRefListInHandles(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition);
	
	public Dataset<Employee> getEmployeeRefListInHandlesByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getEmployeeRefListInHandles(order_condition, null);
	}
	
	public Employee getEmployeeRefInHandlesByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.orderID,Operator.EQUALS, order.getOrderID());
		Dataset<Employee> res = getEmployeeRefListInHandlesByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Employee> getEmployeeRefListInHandlesByEmployeeRefCondition(conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition){
		return getEmployeeRefListInHandles(null, employeeRef_condition);
	}
	
	
	public abstract boolean insertEmployee(Employee employee);
	
	public abstract boolean insertEmployeeInEmployeesFromMyMongoDB(Employee employee); 
	private boolean inUpdateMethod = false;
	private List<Row> allEmployeeIdList = null;
	public abstract void updateEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition, conditions.SetClause<conditions.EmployeeAttribute> set);
	
	public void updateEmployee(pojo.Employee employee) {
		//TODO using the id
		return;
	}
	public abstract void updateEmployeeRefListInHandles(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition,
		
		conditions.SetClause<conditions.EmployeeAttribute> set
	);
	
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
	
	
	public abstract void deleteEmployeeList(conditions.Condition<conditions.EmployeeAttribute> condition);
	
	public void deleteEmployee(pojo.Employee employee) {
		//TODO using the id
		return;
	}
	public abstract void deleteEmployeeRefListInHandles(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.EmployeeAttribute> employeeRef_condition);
	
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
