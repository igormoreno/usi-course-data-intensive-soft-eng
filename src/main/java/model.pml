physical schemas myGroupSql {
	
	
	
	relational schema reldata  {
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
