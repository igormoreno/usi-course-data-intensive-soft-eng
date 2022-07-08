package conditions;

import pojo.*;

public class SimpleCondition<E> extends Condition<E> {

	private E attribute;
	private Operator operator;
	private Object value;

	public SimpleCondition(E attribute, Operator operator, Object value) {
		setAttribute(attribute);
		setOperator(operator);
		setValue(value);
	}

	public E getAttribute() {
		return this.attribute;
	}

	public void setAttribute(E attribute) {
		this.attribute = attribute;
	}

	public Operator getOperator() {
		return this.operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return this.value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public boolean hasOrCondition() {
		return false;
	}

	@Override
	public Class<E> eval() throws Exception {
		if(getOperator() == null)
			throw new Exception("You cannot specify a NULL operator in a simple condition");
		if(getValue() == null && operator != Operator.EQUALS && operator != Operator.NOT_EQUALS)
			throw new Exception("You cannot specify a NULL value with this operator");

		return (Class<E>) attribute.getClass();
	}

	@Override
	public boolean evaluate(IPojo obj) {
		if(obj instanceof Product)
			return evaluateProduct((Product) obj);
		if(obj instanceof Customer)
			return evaluateCustomer((Customer) obj);
		if(obj instanceof Employee)
			return evaluateEmployee((Employee) obj);
		if(obj instanceof Order)
			return evaluateOrder((Order) obj);
		if(obj instanceof Supplier)
			return evaluateSupplier((Supplier) obj);
		if(obj instanceof Composed_of)
			return evaluateComposed_of((Composed_of) obj);
		return true;
	}


	private boolean evaluateProduct(Product obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ProductAttribute attr = (ProductAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ProductAttribute.productID)
			objectValue = obj.getProductID();
		if(attr == ProductAttribute.unitsInStock)
			objectValue = obj.getUnitsInStock();
		if(attr == ProductAttribute.unitsOnOrder)
			objectValue = obj.getUnitsOnOrder();
		if(attr == ProductAttribute.productName)
			objectValue = obj.getProductName();
		if(attr == ProductAttribute.quantityPerUnit)
			objectValue = obj.getQuantityPerUnit();
		if(attr == ProductAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ProductAttribute.reorderLevel)
			objectValue = obj.getReorderLevel();
		if(attr == ProductAttribute.discontinued)
			objectValue = obj.getDiscontinued();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateCustomer(Customer obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		CustomerAttribute attr = (CustomerAttribute) this.attribute;
		Object objectValue = null;

		if(attr == CustomerAttribute.iD)
			objectValue = obj.getID();
		if(attr == CustomerAttribute.address)
			objectValue = obj.getAddress();
		if(attr == CustomerAttribute.city)
			objectValue = obj.getCity();
		if(attr == CustomerAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == CustomerAttribute.contactName)
			objectValue = obj.getContactName();
		if(attr == CustomerAttribute.contactTitle)
			objectValue = obj.getContactTitle();
		if(attr == CustomerAttribute.country)
			objectValue = obj.getCountry();
		if(attr == CustomerAttribute.fax)
			objectValue = obj.getFax();
		if(attr == CustomerAttribute.phone)
			objectValue = obj.getPhone();
		if(attr == CustomerAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == CustomerAttribute.region)
			objectValue = obj.getRegion();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateEmployee(Employee obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		EmployeeAttribute attr = (EmployeeAttribute) this.attribute;
		Object objectValue = null;

		if(attr == EmployeeAttribute.employeeID)
			objectValue = obj.getEmployeeID();
		if(attr == EmployeeAttribute.address)
			objectValue = obj.getAddress();
		if(attr == EmployeeAttribute.birthDate)
			objectValue = obj.getBirthDate();
		if(attr == EmployeeAttribute.city)
			objectValue = obj.getCity();
		if(attr == EmployeeAttribute.country)
			objectValue = obj.getCountry();
		if(attr == EmployeeAttribute.extension)
			objectValue = obj.getExtension();
		if(attr == EmployeeAttribute.firstName)
			objectValue = obj.getFirstName();
		if(attr == EmployeeAttribute.hireDate)
			objectValue = obj.getHireDate();
		if(attr == EmployeeAttribute.homePhone)
			objectValue = obj.getHomePhone();
		if(attr == EmployeeAttribute.lastName)
			objectValue = obj.getLastName();
		if(attr == EmployeeAttribute.notes)
			objectValue = obj.getNotes();
		if(attr == EmployeeAttribute.photo)
			objectValue = obj.getPhoto();
		if(attr == EmployeeAttribute.photoPath)
			objectValue = obj.getPhotoPath();
		if(attr == EmployeeAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == EmployeeAttribute.region)
			objectValue = obj.getRegion();
		if(attr == EmployeeAttribute.salary)
			objectValue = obj.getSalary();
		if(attr == EmployeeAttribute.title)
			objectValue = obj.getTitle();
		if(attr == EmployeeAttribute.titleOfCourtesy)
			objectValue = obj.getTitleOfCourtesy();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateOrder(Order obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		OrderAttribute attr = (OrderAttribute) this.attribute;
		Object objectValue = null;

		if(attr == OrderAttribute.orderID)
			objectValue = obj.getOrderID();
		if(attr == OrderAttribute.freight)
			objectValue = obj.getFreight();
		if(attr == OrderAttribute.orderDate)
			objectValue = obj.getOrderDate();
		if(attr == OrderAttribute.requiredDate)
			objectValue = obj.getRequiredDate();
		if(attr == OrderAttribute.shipAddress)
			objectValue = obj.getShipAddress();
		if(attr == OrderAttribute.shipCity)
			objectValue = obj.getShipCity();
		if(attr == OrderAttribute.shipCountry)
			objectValue = obj.getShipCountry();
		if(attr == OrderAttribute.shipName)
			objectValue = obj.getShipName();
		if(attr == OrderAttribute.shipPostalCode)
			objectValue = obj.getShipPostalCode();
		if(attr == OrderAttribute.shipRegion)
			objectValue = obj.getShipRegion();
		if(attr == OrderAttribute.shippedDate)
			objectValue = obj.getShippedDate();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateSupplier(Supplier obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		SupplierAttribute attr = (SupplierAttribute) this.attribute;
		Object objectValue = null;

		if(attr == SupplierAttribute.supplierID)
			objectValue = obj.getSupplierID();
		if(attr == SupplierAttribute.address)
			objectValue = obj.getAddress();
		if(attr == SupplierAttribute.city)
			objectValue = obj.getCity();
		if(attr == SupplierAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == SupplierAttribute.contactName)
			objectValue = obj.getContactName();
		if(attr == SupplierAttribute.contactTitle)
			objectValue = obj.getContactTitle();
		if(attr == SupplierAttribute.country)
			objectValue = obj.getCountry();
		if(attr == SupplierAttribute.fax)
			objectValue = obj.getFax();
		if(attr == SupplierAttribute.homePage)
			objectValue = obj.getHomePage();
		if(attr == SupplierAttribute.phone)
			objectValue = obj.getPhone();
		if(attr == SupplierAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == SupplierAttribute.region)
			objectValue = obj.getRegion();

		return operator.evaluate(objectValue, this.getValue());
	}
		private boolean evaluateComposed_of(Composed_of obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		Composed_ofAttribute attr = (Composed_ofAttribute) this.attribute;
		Object objectValue = null;

		if(attr == Composed_ofAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == Composed_ofAttribute.quantity)
			objectValue = obj.getQuantity();
		if(attr == Composed_ofAttribute.discount)
			objectValue = obj.getDiscount();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
