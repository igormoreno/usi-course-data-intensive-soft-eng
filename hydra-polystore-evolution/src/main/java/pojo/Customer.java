package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Customer extends LoggingPojo {

	private String iD;
	private String address;
	private String city;
	private String companyName;
	private String contactName;
	private String contactTitle;
	private String country;
	private String fax;
	private String phone;
	private String postalCode;
	private String region;

	public enum buys {
		customerRef
	}
	private List<Order> boughtOrderList;

	// Empty constructor
	public Customer() {}

	// Constructor on Identifier
	public Customer(String iD){
		this.iD = iD;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Customer(String iD,String address,String city,String companyName,String contactName,String contactTitle,String country,String fax,String phone,String postalCode,String region) {
		this.iD = iD;
		this.address = address;
		this.city = city;
		this.companyName = companyName;
		this.contactName = contactName;
		this.contactTitle = contactTitle;
		this.country = country;
		this.fax = fax;
		this.phone = phone;
		this.postalCode = postalCode;
		this.region = region;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Customer Customer = (Customer) o;
		boolean eqSimpleAttr = Objects.equals(iD,Customer.iD) && Objects.equals(address,Customer.address) && Objects.equals(city,Customer.city) && Objects.equals(companyName,Customer.companyName) && Objects.equals(contactName,Customer.contactName) && Objects.equals(contactTitle,Customer.contactTitle) && Objects.equals(country,Customer.country) && Objects.equals(fax,Customer.fax) && Objects.equals(phone,Customer.phone) && Objects.equals(postalCode,Customer.postalCode) && Objects.equals(region,Customer.region);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(boughtOrderList, Customer.boughtOrderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Customer { " + "iD="+iD +", "+
					"address="+address +", "+
					"city="+city +", "+
					"companyName="+companyName +", "+
					"contactName="+contactName +", "+
					"contactTitle="+contactTitle +", "+
					"country="+country +", "+
					"fax="+fax +", "+
					"phone="+phone +", "+
					"postalCode="+postalCode +", "+
					"region="+region +"}"; 
	}
	
	public String getID() {
		return iD;
	}

	public void setID(String iD) {
		this.iD = iD;
	}
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public String getContactName() {
		return contactName;
	}

	public void setContactName(String contactName) {
		this.contactName = contactName;
	}
	public String getContactTitle() {
		return contactTitle;
	}

	public void setContactTitle(String contactTitle) {
		this.contactTitle = contactTitle;
	}
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
	public String getFax() {
		return fax;
	}

	public void setFax(String fax) {
		this.fax = fax;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
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

	

	public List<Order> _getBoughtOrderList() {
		return boughtOrderList;
	}

	public void _setBoughtOrderList(List<Order> boughtOrderList) {
		this.boughtOrderList = boughtOrderList;
	}
}
