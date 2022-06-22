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
	
	
	
	
	relational schema reldata : reldata {
		table Order_details {
			columns {
				OrderRef,
				ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			references {
				productRef : ProductRef -> ProductsInfo.ProductID
				unitPrice : UnitPrice -> ProductsInfo.UnitPrice
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
				productRef : ProductID -> Order_details.ProductRef
				unitPrice : UnitPrice -> Order_details.UnitPrice
			}
			
		}
		
	}
}
