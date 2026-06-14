from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#create rules
customers_rules = {
    'email_format': "email like '%@%'"
}
# create Streaming customers views
@dp.view()
@dp.expect_all_or_drop(customers_rules)
def customers_view():
    df = spark.readStream.format('delta')\
        .table('silver.customers')
    return df

# create target table
dp.create_streaming_table(
    name = 'customers'
)

# SCD type 1
dp.create_auto_cdc_flow(
    target = 'customers',
    source = 'customers_view',
    keys = ['customer_id'],
    sequence_by = col('created_at'),
    stored_as_scd_type=1
)

# create products rules
products_rules = {
    'price_positive': "price is NOT NULL and typeof(price) like 'decimal%' and price >0 "
}

# Create Streaming view products
@dp.view()
# @dp.expect_all(products_rules)
def products_view():
    df = spark.readStream.format('delta')\
        .table('silver.products')
    return df

# create target table
dp.create_streaming_table(
    name = 'products'
)

#scd type 2
dp.create_auto_cdc_flow(
    target = 'products',
    source= 'products_view',
    keys = ['product_id'],
    sequence_by = col('created_at'),
    stored_as_scd_type=2
)

# Streaming order view
@dp.view()
def orders_view():
    df = spark.readStream.format('delta')\
        .table('silver.orders')
    return df
# Streaming order table
@dp.table()
def orders():
    df_orders = spark.readStream.table('orders_view')
    customers = spark.read.format('delta')\
        .table('customers')
    products = spark.read.format('delta')\
        .table('products')
    # left join orders with customers and products
    df_merge = (
        df_orders.alias('o')
            .join(customers.alias('c'), on="customer_id", how="left")
            .join(
                products.alias('p'),
                (col("o.product_id") == col("p.product_id")) &
                (col("p.__END_AT").isNull()),
                how="left"
            )
    ).select(
        col("o.id").alias("order_id"),
        col("c.id").alias("customer_id"),
        col("p.id").alias("product_id"),
        col("o.order_date"),
        col("o.quantity"),
        col("o.total_amount"),
        col("o.created_at"),
        col("o.year"),
        col("o.dense_rank"),
        col("o.rank"),
        col("o.row_number"))
    return df_merge
    
        

