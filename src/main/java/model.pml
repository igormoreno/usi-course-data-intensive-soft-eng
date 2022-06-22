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
			references {
				stock : ProductID -> reldata.ProductsInfo.ProductID
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
				CategoryRef,
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
