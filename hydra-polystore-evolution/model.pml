databases {
	mongodb myMongoDB {
		host : "hydra.unamurcs.be"
		port : 27014
	}
	
	redis myRedis {
		host : "hydra.unamurcs.be"
		port : 63794
	}
}

physical schemas {
	
	key value schema myRedis : myRedis {
		kvpairs product {
			key : "PRODUCT:"[ProductID],
			value : hash {
				ProductName,
				QuantityPerUnit,
				UnitPrice,
				ReorderLevel,
				Discontinued,
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
				},
				products[0-N] {
					ProductID,
					ProductName,
					UnitPrice,
					Quantity,
					Discount
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
				Region,
				products[] : ProductRef
			}
			references {
				prodSupplied: ProductRef -> myRedis.product.ProductID	
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
		hireDate : date,
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
		orderDate: date,
		requiredDate : date,
		shipAddress : string,
		shipCity : string,
		shipCountry : string,
		shipName : string,
		shipPostalCode : string,
		shipRegion : string,
		shippedDate : date
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
	// many-to-many - nested entities
	group2.supplies.supplierRef
		 -> myMongoDB.Suppliers.prodSupplied,

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
	
	// single entity - single database
	group2.Product(productID, productName, quantityPerUnit,
		           unitPrice, reorderLevel, discontinued, unitsInStock, unitsOnOrder)
	    -> myRedis.product(ProductID, ProductName, QuantityPerUnit,
		           UnitPrice, ReorderLevel, Discontinued, UnitsInStock, UnitsOnOrder),
	// many-to-many with attributes - nested entities 
	// answer taken from: nested, many-to-many with entities
	group2.Product(productID, productName)
		 -> myMongoDB.Orders.products(ProductID, ProductName),
	group2.composed_of.productRef
		 -> myMongoDB.Orders.products(),
	rel : group2.composed_of(unitPrice, quantity, discount)
	    -> myMongoDB.Orders.products(UnitPrice, Quantity, Discount)
		 
}
