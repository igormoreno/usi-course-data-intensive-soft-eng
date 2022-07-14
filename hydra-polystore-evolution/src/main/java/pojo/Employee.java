package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Employee extends LoggingPojo {

	private Integer employeeID;
	private String address;
	private LocalDate birthDate;
	private String city;
	private String country;
	private String extension;
	private String firstName;
	private LocalDate hireDate;
	private String homePhone;
	private String lastName;
	private String notes;
	private byte[] photo;
	private String photoPath;
	private String postalCode;
	private String region;
	private Double salary;
	private String title;
	private String titleOfCourtesy;

	public enum handles {
		employeeRef
	}
	private List<Order> orderList;

	// Empty constructor
	public Employee() {}

	// Constructor on Identifier
	public Employee(Integer employeeID){
		this.employeeID = employeeID;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Employee(Integer employeeID,String address,LocalDate birthDate,String city,String country,String extension,String firstName,LocalDate hireDate,String homePhone,String lastName,String notes,byte[] photo,String photoPath,String postalCode,String region,Double salary,String title,String titleOfCourtesy) {
		this.employeeID = employeeID;
		this.address = address;
		this.birthDate = birthDate;
		this.city = city;
		this.country = country;
		this.extension = extension;
		this.firstName = firstName;
		this.hireDate = hireDate;
		this.homePhone = homePhone;
		this.lastName = lastName;
		this.notes = notes;
		this.photo = photo;
		this.photoPath = photoPath;
		this.postalCode = postalCode;
		this.region = region;
		this.salary = salary;
		this.title = title;
		this.titleOfCourtesy = titleOfCourtesy;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Employee Employee = (Employee) o;
		boolean eqSimpleAttr = Objects.equals(employeeID,Employee.employeeID) && Objects.equals(address,Employee.address) && Objects.equals(birthDate,Employee.birthDate) && Objects.equals(city,Employee.city) && Objects.equals(country,Employee.country) && Objects.equals(extension,Employee.extension) && Objects.equals(firstName,Employee.firstName) && Objects.equals(hireDate,Employee.hireDate) && Objects.equals(homePhone,Employee.homePhone) && Objects.equals(lastName,Employee.lastName) && Objects.equals(notes,Employee.notes) && Objects.equals(photo,Employee.photo) && Objects.equals(photoPath,Employee.photoPath) && Objects.equals(postalCode,Employee.postalCode) && Objects.equals(region,Employee.region) && Objects.equals(salary,Employee.salary) && Objects.equals(title,Employee.title) && Objects.equals(titleOfCourtesy,Employee.titleOfCourtesy);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(orderList, Employee.orderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Employee { " + "employeeID="+employeeID +", "+
					"address="+address +", "+
					"birthDate="+birthDate +", "+
					"city="+city +", "+
					"country="+country +", "+
					"extension="+extension +", "+
					"firstName="+firstName +", "+
					"hireDate="+hireDate +", "+
					"homePhone="+homePhone +", "+
					"lastName="+lastName +", "+
					"notes="+notes +", "+
					"photo="+photo +", "+
					"photoPath="+photoPath +", "+
					"postalCode="+postalCode +", "+
					"region="+region +", "+
					"salary="+salary +", "+
					"title="+title +", "+
					"titleOfCourtesy="+titleOfCourtesy +"}"; 
	}
	
	public Integer getEmployeeID() {
		return employeeID;
	}

	public void setEmployeeID(Integer employeeID) {
		this.employeeID = employeeID;
	}
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	public LocalDate getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(LocalDate birthDate) {
		this.birthDate = birthDate;
	}
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public LocalDate getHireDate() {
		return hireDate;
	}

	public void setHireDate(LocalDate hireDate) {
		this.hireDate = hireDate;
	}
	public String getHomePhone() {
		return homePhone;
	}

	public void setHomePhone(String homePhone) {
		this.homePhone = homePhone;
	}
	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}
	public byte[] getPhoto() {
		return photo;
	}

	public void setPhoto(byte[] photo) {
		this.photo = photo;
	}
	public String getPhotoPath() {
		return photoPath;
	}

	public void setPhotoPath(String photoPath) {
		this.photoPath = photoPath;
	}
	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}
	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}
	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	public String getTitleOfCourtesy() {
		return titleOfCourtesy;
	}

	public void setTitleOfCourtesy(String titleOfCourtesy) {
		this.titleOfCourtesy = titleOfCourtesy;
	}

	

	public List<Order> _getOrderList() {
		return orderList;
	}

	public void _setOrderList(List<Order> orderList) {
		this.orderList = orderList;
	}
}
