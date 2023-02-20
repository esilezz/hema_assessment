from pyspark.sql.types import FloatType, IntegerType, StringType

dtypes = {
    'rowId': IntegerType(),
    'orderId': StringType(),
    'shipMode': StringType(),
    'customerId': StringType(),
    'customerName': StringType(),
    'segment': StringType(),
    'country': StringType(),
    'city': StringType(),
    'state': StringType(),
    'postalCode': IntegerType(),
    'region': StringType(),
    'productId': StringType(), 
    'category': StringType(),
    'subCategory': StringType(),
    'productName': StringType(),
    'sales': FloatType()
}