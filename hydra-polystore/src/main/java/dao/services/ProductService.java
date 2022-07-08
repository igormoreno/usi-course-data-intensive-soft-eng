package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Product;
import conditions.ProductAttribute;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.ProductAttribute;
import pojo.Composed_of;
import conditions.OrderAttribute;
import pojo.Order;
import conditions.ProductAttribute;
import pojo.Supplies;
import conditions.SupplierAttribute;
import pojo.Supplier;

public abstract class ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductService.class);
	protected Composed_ofService composed_ofService = new dao.impl.Composed_ofServiceImpl();
	protected SuppliesService suppliesService = new dao.impl.SuppliesServiceImpl();
	


	public static enum ROLE_NAME {
		COMPOSED_OF_PRODUCTREF, SUPPLIES_SUPPLIEDPRODUCT
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.COMPOSED_OF_PRODUCTREF, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.SUPPLIES_SUPPLIEDPRODUCT, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ProductService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ProductService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Product> getProductList(){
		return getProductList(null);
	}
	
	public Dataset<Product> getProductList(conditions.Condition<conditions.ProductAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Product>> datasets = new ArrayList<Dataset<Product>>();
		Dataset<Product> d = null;
		d = getProductListInProductStockInfoFromMyRedis(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductListInProductsInfoFromReldata(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsProduct(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"productID"});
		return d;
	}
	
	
	
	
	
	public abstract Dataset<Product> getProductListInProductStockInfoFromMyRedis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	public abstract Dataset<Product> getProductListInProductsInfoFromReldata(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Product getProductById(Integer productID){
		Condition cond;
		cond = Condition.simple(ProductAttribute.productID, conditions.Operator.EQUALS, productID);
		Dataset<Product> res = getProductList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Product> getProductListByProductID(Integer productID) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.productID, conditions.Operator.EQUALS, productID));
	}
	
	public Dataset<Product> getProductListByUnitsInStock(Integer unitsInStock) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.unitsInStock, conditions.Operator.EQUALS, unitsInStock));
	}
	
	public Dataset<Product> getProductListByUnitsOnOrder(Integer unitsOnOrder) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.unitsOnOrder, conditions.Operator.EQUALS, unitsOnOrder));
	}
	
	public Dataset<Product> getProductListByProductName(String productName) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.productName, conditions.Operator.EQUALS, productName));
	}
	
	public Dataset<Product> getProductListByQuantityPerUnit(String quantityPerUnit) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.quantityPerUnit, conditions.Operator.EQUALS, quantityPerUnit));
	}
	
	public Dataset<Product> getProductListByUnitPrice(Double unitPrice) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.unitPrice, conditions.Operator.EQUALS, unitPrice));
	}
	
	public Dataset<Product> getProductListByReorderLevel(Integer reorderLevel) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.reorderLevel, conditions.Operator.EQUALS, reorderLevel));
	}
	
	public Dataset<Product> getProductListByDiscontinued(Boolean discontinued) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.discontinued, conditions.Operator.EQUALS, discontinued));
	}
	
	
	
	public static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Product> fullLeftOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Product> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("productID");
			logger.debug("Start {} of [{}] datasets of [Product] objects",joinMode,datasetsPOJO.size());
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("unitsInStock", "unitsInStock_1")
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_1")
								.withColumnRenamed("productName", "productName_1")
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_1")
								.withColumnRenamed("unitPrice", "unitPrice_1")
								.withColumnRenamed("reorderLevel", "reorderLevel_1")
								.withColumnRenamed("discontinued", "discontinued_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("unitsInStock", "unitsInStock_" + i)
								.withColumnRenamed("unitsOnOrder", "unitsOnOrder_" + i)
								.withColumnRenamed("productName", "productName_" + i)
								.withColumnRenamed("quantityPerUnit", "quantityPerUnit_" + i)
								.withColumnRenamed("unitPrice", "unitPrice_" + i)
								.withColumnRenamed("reorderLevel", "reorderLevel_" + i)
								.withColumnRenamed("discontinued", "discontinued_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			}
			logger.debug("End join. Start");
			logger.debug("Start transforming Row objects to [Product] objects"); 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.productID'
					Integer firstNotNull_productID = Util.getIntegerValue(r.getAs("productID"));
					product_res.setProductID(firstNotNull_productID);
					
					// attribute 'Product.unitsInStock'
					Integer firstNotNull_unitsInStock = Util.getIntegerValue(r.getAs("unitsInStock"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsInStock2 = Util.getIntegerValue(r.getAs("unitsInStock_" + i));
						if (firstNotNull_unitsInStock != null && unitsInStock2 != null && !firstNotNull_unitsInStock.equals(unitsInStock2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_unitsInStock + " and " + unitsInStock2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.unitsInStock': " + firstNotNull_unitsInStock + " and " + unitsInStock2 + "." );
						}
						if (firstNotNull_unitsInStock == null && unitsInStock2 != null) {
							firstNotNull_unitsInStock = unitsInStock2;
						}
					}
					product_res.setUnitsInStock(firstNotNull_unitsInStock);
					
					// attribute 'Product.unitsOnOrder'
					Integer firstNotNull_unitsOnOrder = Util.getIntegerValue(r.getAs("unitsOnOrder"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer unitsOnOrder2 = Util.getIntegerValue(r.getAs("unitsOnOrder_" + i));
						if (firstNotNull_unitsOnOrder != null && unitsOnOrder2 != null && !firstNotNull_unitsOnOrder.equals(unitsOnOrder2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_unitsOnOrder + " and " + unitsOnOrder2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.unitsOnOrder': " + firstNotNull_unitsOnOrder + " and " + unitsOnOrder2 + "." );
						}
						if (firstNotNull_unitsOnOrder == null && unitsOnOrder2 != null) {
							firstNotNull_unitsOnOrder = unitsOnOrder2;
						}
					}
					product_res.setUnitsOnOrder(firstNotNull_unitsOnOrder);
					
					// attribute 'Product.productName'
					String firstNotNull_productName = Util.getStringValue(r.getAs("productName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String productName2 = Util.getStringValue(r.getAs("productName_" + i));
						if (firstNotNull_productName != null && productName2 != null && !firstNotNull_productName.equals(productName2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.productName': " + firstNotNull_productName + " and " + productName2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.productName': " + firstNotNull_productName + " and " + productName2 + "." );
						}
						if (firstNotNull_productName == null && productName2 != null) {
							firstNotNull_productName = productName2;
						}
					}
					product_res.setProductName(firstNotNull_productName);
					
					// attribute 'Product.quantityPerUnit'
					String firstNotNull_quantityPerUnit = Util.getStringValue(r.getAs("quantityPerUnit"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String quantityPerUnit2 = Util.getStringValue(r.getAs("quantityPerUnit_" + i));
						if (firstNotNull_quantityPerUnit != null && quantityPerUnit2 != null && !firstNotNull_quantityPerUnit.equals(quantityPerUnit2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_quantityPerUnit + " and " + quantityPerUnit2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.quantityPerUnit': " + firstNotNull_quantityPerUnit + " and " + quantityPerUnit2 + "." );
						}
						if (firstNotNull_quantityPerUnit == null && quantityPerUnit2 != null) {
							firstNotNull_quantityPerUnit = quantityPerUnit2;
						}
					}
					product_res.setQuantityPerUnit(firstNotNull_quantityPerUnit);
					
					// attribute 'Product.unitPrice'
					Double firstNotNull_unitPrice = Util.getDoubleValue(r.getAs("unitPrice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double unitPrice2 = Util.getDoubleValue(r.getAs("unitPrice_" + i));
						if (firstNotNull_unitPrice != null && unitPrice2 != null && !firstNotNull_unitPrice.equals(unitPrice2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.unitPrice': " + firstNotNull_unitPrice + " and " + unitPrice2 + "." );
						}
						if (firstNotNull_unitPrice == null && unitPrice2 != null) {
							firstNotNull_unitPrice = unitPrice2;
						}
					}
					product_res.setUnitPrice(firstNotNull_unitPrice);
					
					// attribute 'Product.reorderLevel'
					Integer firstNotNull_reorderLevel = Util.getIntegerValue(r.getAs("reorderLevel"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer reorderLevel2 = Util.getIntegerValue(r.getAs("reorderLevel_" + i));
						if (firstNotNull_reorderLevel != null && reorderLevel2 != null && !firstNotNull_reorderLevel.equals(reorderLevel2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_reorderLevel + " and " + reorderLevel2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.reorderLevel': " + firstNotNull_reorderLevel + " and " + reorderLevel2 + "." );
						}
						if (firstNotNull_reorderLevel == null && reorderLevel2 != null) {
							firstNotNull_reorderLevel = reorderLevel2;
						}
					}
					product_res.setReorderLevel(firstNotNull_reorderLevel);
					
					// attribute 'Product.discontinued'
					Boolean firstNotNull_discontinued = Util.getBooleanValue(r.getAs("discontinued"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean discontinued2 = Util.getBooleanValue(r.getAs("discontinued_" + i));
						if (firstNotNull_discontinued != null && discontinued2 != null && !firstNotNull_discontinued.equals(discontinued2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_discontinued + " and " + discontinued2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getProductID()+"]: different values found for attribute 'Product.discontinued': " + firstNotNull_discontinued + " and " + discontinued2 + "." );
						}
						if (firstNotNull_discontinued == null && discontinued2 != null) {
							firstNotNull_discontinued = discontinued2;
						}
					}
					product_res.setDiscontinued(firstNotNull_discontinued);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							product_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							product_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return product_res;
				}, Encoders.bean(Product.class));
			return d;
	}
	
	
	
	
	public Dataset<Product> getProductList(Product.supplies role, Supplier supplier) {
		if(role != null) {
			if(role.equals(Product.supplies.suppliedProduct))
				return getSuppliedProductListInSuppliesBySupplierRef(supplier);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.supplies role, Condition<SupplierAttribute> condition) {
		if(role != null) {
			if(role.equals(Product.supplies.suppliedProduct))
				return getSuppliedProductListInSuppliesBySupplierRefCondition(condition);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.supplies role, Condition<ProductAttribute> condition1, Condition<SupplierAttribute> condition2) {
		if(role != null) {
			if(role.equals(Product.supplies.suppliedProduct))
				return getSuppliedProductListInSupplies(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Product> getProductRefListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderRef_condition,conditions.Condition<conditions.ProductAttribute> productRef_condition, conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition);
	
	public Dataset<Product> getProductRefListInComposed_ofByOrderRefCondition(conditions.Condition<conditions.OrderAttribute> orderRef_condition){
		return getProductRefListInComposed_of(orderRef_condition, null, null);
	}
	
	public Dataset<Product> getProductRefListInComposed_ofByOrderRef(pojo.Order orderRef){
		if(orderRef == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.orderID,Operator.EQUALS, orderRef.getOrderID());
		Dataset<Product> res = getProductRefListInComposed_ofByOrderRefCondition(c);
		return res;
	}
	
	public Dataset<Product> getProductRefListInComposed_ofByProductRefCondition(conditions.Condition<conditions.ProductAttribute> productRef_condition){
		return getProductRefListInComposed_of(null, productRef_condition, null);
	}
	public Dataset<Product> getProductRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		return getProductRefListInComposed_of(null, null, composed_of_condition);
	}
	public abstract Dataset<Product> getSuppliedProductListInSupplies(conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,conditions.Condition<conditions.SupplierAttribute> supplierRef_condition);
	
	public Dataset<Product> getSuppliedProductListInSuppliesBySuppliedProductCondition(conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition){
		return getSuppliedProductListInSupplies(suppliedProduct_condition, null);
	}
	public Dataset<Product> getSuppliedProductListInSuppliesBySupplierRefCondition(conditions.Condition<conditions.SupplierAttribute> supplierRef_condition){
		return getSuppliedProductListInSupplies(null, supplierRef_condition);
	}
	
	public Dataset<Product> getSuppliedProductListInSuppliesBySupplierRef(pojo.Supplier supplierRef){
		if(supplierRef == null)
			return null;
	
		Condition c;
		c=Condition.simple(SupplierAttribute.supplierID,Operator.EQUALS, supplierRef.getSupplierID());
		Dataset<Product> res = getSuppliedProductListInSuppliesBySupplierRefCondition(c);
		return res;
	}
	
	
	public abstract boolean insertProduct(
		Product product,
		Supplier	supplierRefSupplies);
	
	public abstract boolean insertProductInProductStockInfoFromMyRedis(Product product); 
	public abstract boolean insertProductInProductsInfoFromReldata(Product product,
		Supplier	supplierRefSupplies);
	private boolean inUpdateMethod = false;
	private List<Row> allProductIdList = null;
	public abstract void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set);
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void updateProductRefListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of,
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
	public void updateProductRefListInComposed_ofByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductRefListInComposed_of(orderRef_condition, null, null, set);
	}
	
	public void updateProductRefListInComposed_ofByOrderRef(
		pojo.Order orderRef,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateProductRefListInComposed_ofByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductRefListInComposed_of(null, productRef_condition, null, set);
	}
	public void updateProductRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateProductRefListInComposed_of(null, null, composed_of_condition, set);
	}
	public abstract void updateSuppliedProductListInSupplies(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
	public void updateSuppliedProductListInSuppliesBySuppliedProductCondition(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateSuppliedProductListInSupplies(suppliedProduct_condition, null, set);
	}
	public void updateSuppliedProductListInSuppliesBySupplierRefCondition(
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateSuppliedProductListInSupplies(null, supplierRef_condition, set);
	}
	
	public void updateSuppliedProductListInSuppliesBySupplierRef(
		pojo.Supplier supplierRef,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition);
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void deleteProductRefListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderRef_condition,	
		conditions.Condition<conditions.ProductAttribute> productRef_condition,
		conditions.Condition<conditions.Composed_ofAttribute> composed_of);
	
	public void deleteProductRefListInComposed_ofByOrderRefCondition(
		conditions.Condition<conditions.OrderAttribute> orderRef_condition
	){
		deleteProductRefListInComposed_of(orderRef_condition, null, null);
	}
	
	public void deleteProductRefListInComposed_ofByOrderRef(
		pojo.Order orderRef 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteProductRefListInComposed_ofByProductRefCondition(
		conditions.Condition<conditions.ProductAttribute> productRef_condition
	){
		deleteProductRefListInComposed_of(null, productRef_condition, null);
	}
	public void deleteProductRefListInComposed_ofByComposed_ofCondition(
		conditions.Condition<conditions.Composed_ofAttribute> composed_of_condition
	){
		deleteProductRefListInComposed_of(null, null, composed_of_condition);
	}
	public abstract void deleteSuppliedProductListInSupplies(	
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition,	
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition);
	
	public void deleteSuppliedProductListInSuppliesBySuppliedProductCondition(
		conditions.Condition<conditions.ProductAttribute> suppliedProduct_condition
	){
		deleteSuppliedProductListInSupplies(suppliedProduct_condition, null);
	}
	public void deleteSuppliedProductListInSuppliesBySupplierRefCondition(
		conditions.Condition<conditions.SupplierAttribute> supplierRef_condition
	){
		deleteSuppliedProductListInSupplies(null, supplierRef_condition);
	}
	
	public void deleteSuppliedProductListInSuppliesBySupplierRef(
		pojo.Supplier supplierRef 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
