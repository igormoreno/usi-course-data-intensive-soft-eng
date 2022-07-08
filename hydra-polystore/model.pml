databases {
	mysql reldata {
		dbname : "reldata"
		host : "hydra.unamurcs.be"
		login : "root"
		password : "password"
		port : 33063
	}
	
	mongodb myMongoDB {
		host : "hydra.unamurcs.be"
		port : 27013
	}
	
	redis myRedis {
		host : "hydra.unamurcs.be"
		port : 63793		
	}
}

physical schemas {
	
	key value schema myRedis : myRedis {
		kvpairs productStockInfo {
			key : "PRODUCT:"[ProductID]":STOCKINFO",
			value : hash {
				UnitsInStock,
				UnitsOnOrder
			}
		}
	}
	
	document schema myMongoDB : myMongoDB {
		collection Customers {
			fields {
				ID,
				Address,
				City,
				CompanyName,
				ContactName,
				ContactTitle,
				Country,
				Fax,
				Phone,
				PostalCode,
				Region
			}
		}
		
		collection Employees {
			fields {
				EmployeeID,
				Address,
				BirthDate,
				City,
				Country,
				Extension,
				FirstName,
				HireDate,
				HomePhone,
				LastName,
				Notes,
				Photo,
				PhotoPath,
				PostalCode,
				Region,
				Salary,
				Title,
				TitleOfCourtesy
			}
		}
		
		collection Orders {
			fields {
				OrderID,
				EmployeeRef,
				Freight,
				OrderDate,
				RequiredDate,
				ShipAddress,
				ShipCity,
				ShipCountry,
				ShipName,
				ShipPostalCode,
				ShipRegion,
				ShippedDate,
				customer[1] {
					CustomerID,
					ContactName
				}
			}
			references {
				orderHandler : EmployeeRef -> myMongoDB.Employees.EmployeeID
			}
		}
		
		collection Suppliers {
			fields {
				SupplierID,
				Address,
				City,
				CompanyName,
				ContactName,
				ContactTitle,
				Country,
				Fax,
				HomePage,
				Phone,
				PostalCode,
				Region
			}
		}
	}
	
	relational schema reldata : reldata {
		table Order_Details {
			columns {
				OrderRef,
				ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			references {
				productRef : ProductRef -> reldata.ProductsInfo.ProductID
				orderRef : OrderRef -> myMongoDB.Orders.OrderID
			}
		}
		
		table ProductsInfo {
			columns {
				ProductID,
				ProductName,
				SupplierRef,
				QuantityPerUnit,
				UnitPrice,
				ReorderLevel,
				Discontinued
			}
			references {
				supplierRef : SupplierRef -> myMongoDB.Suppliers.SupplierID
			}
		}
	}
}

conceptual schema group2 {
	entity type Product {
		productID : int,
		unitsInStock : int,
		unitsOnOrder : int,
		productName : string,
		quantityPerUnit : string,
		unitPrice : float,
		reorderLevel : int,
		discontinued: bool
		identifier {
			productID
		}
	}
	
	entity type Customer {
		iD : string,
		address : string,
		city : string,
		companyName : string,
		contactName : string,
		contactTitle : string,
		country : string,
		fax : string,
		phone : string,
		postalCode : string,
		region : string
		identifier {
			iD
		}
	}
	
	entity type Employee {
		employeeID : int,
		address : string,
		birthDate : date,
		city : string,
		country : string,
		extension : string,
		firstName : string,
		hireDate : datetime,
		homePhone : string,
		lastName : string,
		notes : string,
		photo : blob,
		photoPath : string,
		postalCode : string,
		region : string,
		salary : float,
		title : string,
		titleOfCourtesy : string
		identifier {
			employeeID
		}
	}
	
	entity type Order {
		orderID : int,
		freight : float,
		orderDate: datetime,
		requiredDate : datetime,
		shipAddress : string,
		shipCity : string,
		shipCountry : string,
		shipName : string,
		shipPostalCode : string,
		shipRegion : string,
		shippedDate : datetime
		identifier {
			orderID
		}
	}
	
	entity type Supplier {
		supplierID : int,
		address : string,
		city : string,
		companyName : string,
		contactName : string,
		contactTitle : string,
		country : string,
		fax : string,
		homePage : string,
		phone : string,
		postalCode : string,
		region : string
		identifier {
			supplierID
		}
	}
	
	relationship type composed_of {
		orderRef[1-N] : Order,
		productRef[0-N] : Product,
		unitPrice : float,
		quantity : int,
		discount : float		
	}
	
	relationship type buys {
		boughtOrder[1] : Order,
		customerRef[0-N] : Customer	
	}
	
	relationship type handles {
		order[1] : Order,
		employeeRef[0-N] : Employee
	}
	
	relationship type supplies {
		suppliedProduct[1] : Product,
		supplierRef[0-N] : Supplier		
	}
}

mapping rules {
	group2.Supplier(supplierID, address, city,
		            companyName, contactName, contactTitle,
		            country, fax, homePage, phone,
		            postalCode, region)
		 -> myMongoDB.Suppliers(SupplierID, Address, City,
		 	                    CompanyName, ContactName,
		 	                    ContactTitle, Country, Fax,
		 	                    HomePage, Phone, PostalCode,
		 	                    Region),
	// one-to-many - multiple databases
	group2.supplies.suppliedProduct -> reldata.ProductsInfo.supplierRef,

	group2.Customer(iD, address, city, companyName,
		            contactName, contactTitle, country,
		            fax, phone, postalCode, region)
		 -> myMongoDB.Customers(ID, Address, City, CompanyName,
		 	                    ContactName, ContactTitle, Country,
		 	                    Fax, Phone, PostalCode, Region),
	// one-to-many - nested entities
	group2.buys.boughtOrder -> myMongoDB.Orders.customer(),
	
	// one-to-many - nested entities
	group2.Customer(iD, contactName)
		 -> myMongoDB.Orders.customer(CustomerID, ContactName),

	group2.Employee(employeeID, address, birthDate, city,
		            country, extension, firstName, hireDate,
		            homePhone, lastName, notes, photo,
		            photoPath, postalCode, region, salary,
		            title, titleOfCourtesy)
		 -> myMongoDB.Employees(EmployeeID, Address, BirthDate,
		 	                    City, Country, Extension, FirstName,
		 	                    HireDate, HomePhone, LastName,
		 	                    Notes, Photo, PhotoPath, PostalCode,
		 	                    Region, Salary, Title, TitleOfCourtesy),

	group2.Order(orderID, freight, orderDate, requiredDate,
				 shipAddress, shipCity, shipCountry, shipName,
				 shipPostalCode, shipRegion, shippedDate)
	     -> myMongoDB.Orders(OrderID, Freight, OrderDate,
							 RequiredDate, ShipAddress, ShipCity,
							 ShipCountry, ShipName, ShipPostalCode,
							 ShipRegion, ShippedDate),
	// one-to-many - single database
	group2.handles.order -> myMongoDB.Orders.orderHandler,
	
	// single entity - multiple database
	group2.Product(productID, unitsInStock, unitsOnOrder)
	    -> myRedis.productStockInfo(ProductID, UnitsInStock, UnitsOnOrder),
	group2.Product(productID, productName, quantityPerUnit,
		           unitPrice, reorderLevel, discontinued) 
		-> reldata.ProductsInfo(ProductID, ProductName, QuantityPerUnit,
						 		UnitPrice, ReorderLevel, Discontinued),
	// many-to-many with attributes
	group2.composed_of.orderRef -> reldata.Order_Details.orderRef,
	group2.composed_of.productRef -> reldata.Order_Details.productRef,
	rel : group2.composed_of(unitPrice, quantity, discount)
	    -> reldata.Order_Details(UnitPrice, Quantity, Discount)
}
