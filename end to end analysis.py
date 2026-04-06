# END TO END RETAIL ANALYSIS PROJECT USING DATABRICKS - MEDALLION ARCHITECTURE


#BRONZE LAYER
#STEP 1 : DEFINE SCHEMA AS STRING

from pyspark.sql.types import StructType, StructField, StringType

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("total_amount", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("store_location", StringType(), True),
    StructField("order_status", StringType(), True)
])

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("loyalty_points", StringType(), True),
    StructField("status", StringType(), True)
])

# STEP-2 READ RAW AS CSV ----------

orders_bronze_df = spark.read.format('csv').option("header", "true").schema(orders_schema).load("/Volumes/project/raw_data/raw_file/retail_orders_messy.csv")

customers_bronze_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(customer_schema) \
    .load("/Volumes/project/raw_data/raw_file/retail_customers_messy.csv")

orders_bronze_df.display()
customers_bronze_df.display()

# STEP -3 ADD INGESTION DATA ----------

from pyspark.sql.functions import current_timestamp


orders_bronze_df = orders_bronze_df.withColumn('ingest_timestamp', current_timestamp())
customers_bronze_df = customers_bronze_df.withColumn('ingest_timestamp',current_timestamp())

# STEP-4 SAVE AS DELTA TABLE IN BRONZE ----------

orders_bronze_df.write.format("delta").mode("overwrite").saveAsTable('project.bronze.bronze_orders')
customers_bronze_df.write.format("delta").mode("overwrite").saveAsTable('project.bronze.bronze_customers')

# SILVER LAYER ----------
# STEP 1 : LOAD BRONZE ORDERS

bronze_order_df = spark.read.table("project.bronze.bronze_orders")

display(bronze_order_df)

# STEP - 2 FIX DATE FORMATS ----------

from pyspark.sql.functions import try_to_date, col, coalesce

silver_df = bronze_order_df.withColumn(
    'order_date',
    coalesce(
        try_to_date(col('order_date'),'yyyy-MM-dd'),
        try_to_date(col('order_date'),'dd-MM-yyyy'),
        try_to_date(col('order_date'),'yyyy/MM/dd'),
        try_to_date(col('order_date'),'dd/MM/yyyy')
    )
)
silver_df.display()

# STEP-3 STANDARDIZE CATEGORY VALUES ----------


silver_df.select('category').distinct().display()



from pyspark.sql.functions import upper, trim,when

silver_df = silver_df.withColumn(
    "category",
    upper(trim(col("category")))
)


silver_df = silver_df.withColumn(
        'category',when(col('category')=='ELECTRONIC','ELECTRONICS').
        when(col('category')=='HOME AND LIVING','HOME & LIVING').
        when(col('category')=='FASHON','FASHION').
        otherwise(col('category')))
silver_df.display()

# STEP - 4 REMOVE DUPLICATE ORDERS ----------

from pyspark.sql.functions import count ,countDistinct

silver_df.select(count('order_id'),countDistinct('order_id')).display()

# COMMAND ----------

silver_df = silver_df.dropDuplicates(['order_id'])

silver_df.select(count('order_id'),countDistinct('order_id')).display()

# STEP - 5 HANDLE NULL VALUES DISCOUNT ----------

from pyspark.sql.functions import when, col

silver_df = silver_df.withColumn('discount',
                                 when(col('discount').isNull(),'0').when(col('discount')=='ten','0.1').otherwise(col('discount'))
                                 
                                 )
silver_df.display()

# STEP - 6 CONVERT DISCOUNT PERCENT TO DECIMAL ----------

from pyspark.sql.functions import regexp_replace

silver_df = silver_df.withColumn(
    "discount",
    when(col("discount").like("5%"), 
           (regexp_replace("discount", "5%", "0.05"))
    ).when(col("discount").like("10%"), 
           (regexp_replace("discount", "10%", "0.1"))
    ).otherwise(col("discount"))
)

silver_df.display()

# STEP - 7 REMOVE NEGATIVES IN QUANTITY----------

silver_df = silver_df.filter(col("quantity")>=0)
silver_df.display()
     

# STEP -8 CAST UNIT_PRICE TO DECIMAL ----------

from pyspark.sql.types import DecimalType

silver_df = silver_df.withColumn(
    "unit_price",
    col("unit_price").try_cast(DecimalType(10,2))
)
silver_df.display()

# STEP -9 RECALCULATE TOTAL AMOUNT ----------

silver_df = silver_df.withColumn(
    "total_amount",
    col("quantity").cast("int") * col("unit_price") * (1 - col("discount").cast("double"))
)
silver_df.display()

# STEP - 10 SAVE AS DELTA TABLE ORDERS ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("project.silver.silver_orders")

# STEP - 11 LOAD BRONZE CUSTOMER ----------


customers_bronze_df = spark.table("project.bronze.bronze_customers")

display(customers_bronze_df)

# STEP - 12 TRIM SPACES ----------

from pyspark.sql.functions import trim, col

customers_df = customers_bronze_df.select(
    trim(col("customer_id")).alias("customer_id"),
    trim(col("customer_name")).alias("customer_name"),
    trim(col("email")).alias("email"),
    trim(col("phone")).alias("phone"),
    trim(col("gender")).alias("gender"),
    trim(col("date_of_birth")).alias("date_of_birth"),
    trim(col("city")).alias("city"),
    trim(col("state")).alias("state"),
    trim(col("registration_date")).alias("registration_date"),
    trim(col("loyalty_points")).alias("loyalty_points"),
    trim(col("status")).alias("status"),
    col("ingest_timestamp")
)

display(customers_df)

# STEP - 13 REMOVE DUPLICATE CUSTOMERS ----------



from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = Window.partitionBy("customer_id") \
                    .orderBy(desc("ingest_timestamp"))

customers_df = customers_df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

customers_df = customers_df.filter("row_num = 1") \
                           .drop("row_num")

customers_df.display()
     


# STEP -14 FIX AND STANDARDIZE STATUS----------

from pyspark.sql.functions import upper, when

customers_df = customers_df.withColumn(
    "status",
    upper(col("status"))
)

customers_df = customers_df.withColumn(
    "status",
    when(col("status").isin("ACTIVE", "INACTIVE"), col("status"))
    .otherwise("UNKNOWN")
)

customers_df.display()

# STEP - 15 CONVERT LOYALTY POINTS TO INTEGER----------



from pyspark.sql.functions import regexp_replace

customers_df = customers_df.withColumn(
    "loyalty_points",
    regexp_replace(col("loyalty_points"), "[^0-9]", "")
)

customers_df = customers_df.withColumn(
    "loyalty_points",
    col("loyalty_points").try_cast("int")
)
     

customers_df.display()
     


# STEP - 16 VALIDATE EMAIL FORMAT ----------

customers_df = customers_df.withColumn(
    "email_valid",
    col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
)

# Example: meenadas@email.com - valid, karansingh@email - invalid

# customers_df.select("email", "email_valid").display()

customers_df = customers_df.filter(col("email_valid") == True)






# STEP - 17 FIX DATE FORMAT ----------


from pyspark.sql.functions import to_date, coalesce

customers_df = customers_df.withColumn(
    "dob_parsed",
    coalesce(
            try_to_date(col('date_of_birth'),'yyyy-MM-dd'),
            try_to_date(col('date_of_birth'),'dd-MM-yyyy'),
            try_to_date(col('date_of_birth'),'yyyy/MM/dd'),
            try_to_date(col('date_of_birth'),'dd/MM/yyyy')
        )
)

customers_df = customers_df.withColumn(
    "registration_parsed",
    coalesce(
            try_to_date(col('registration_date'),'yyyy-MM-dd'),
            try_to_date(col('registration_date'),'dd-MM-yyyy'),
            try_to_date(col('registration_date'),'yyyy/MM/dd'),
            try_to_date(col('registration_date'),'dd/MM/yyyy')
        )
)

customers_df.display()
     

customers_df = customers_df.drop("date_of_birth", "registration_date") \
                           .withColumnRenamed("dob_parsed", "date_of_birth") \
                           .withColumnRenamed("registration_parsed", "registration_date")
     

customers_df.display()
     


# STEP -17 STANDARDIZE GENDER ----------

customers_df = customers_df.withColumn(
    "gender",
    upper(col("gender"))
)

customers_df = customers_df.withColumn(
    "gender",
    when(col("gender") == "MALE", "M")
    .when(col("gender") == "FEMALE", "F")
    .otherwise("U")
)

# STEP - 18 SAVE AS DELTA TABLE SILVER ----------

customers_df.write.format("delta").mode("overwrite").saveAsTable("project.silver.silver_customers")



# GOLD LAYER ----------
#STEP -1 LOAD SILVER ORDERS AND CUSTOMERS


customers_df = spark.read.table("project.silver.silver_customers")
orders_df = spark.read.table("project.silver.silver_orders")

customers_df.display()
orders_df.display()

# STEP - 2 CREATE DIMENSION TABLE ----------



dim_customer = customers_df.select("customer_id",
                    "customer_name",
                    "email",
                    "city",
                    "state",
                    "loyalty_points",
                    "status"                    
                    )
dim_customer.display()
     

dim_customer.write.format('delta').mode('overwrite').saveAsTable('project.gold.gold_dim_customer')

# STEP -2 JOINS TABLE ----------

fact_sales = orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "left"
).select(
    orders_df.order_id,
    orders_df.order_date,
    orders_df.customer_id,
    customers_df.customer_name,
    orders_df.product_id,
    orders_df.product_name,
    orders_df.category,
    orders_df.quantity,
    orders_df.unit_price,
    orders_df.discount,
    orders_df.total_amount,
    customers_df.city,
    customers_df.state,
    customers_df.gender,
    customers_df.loyalty_points
)

#STEP -3 fact_sales.display


from pyspark.sql.functions import year , month , dayofmonth

order_enriched = fact_sales.\
    withColumn("Year",year("order_date")).\
    withColumn("Month",month("order_date")).\
    withColumn("Day",dayofmonth("order_date"))

order_enriched.display()


#step - 4 aggergates----------

from pyspark.sql.functions import sum, countDistinct, avg, min,max

gold_df = order_enriched.groupBy(
    "product_id",
    "product_name",
    "customer_id",
    "customer_name",
    "category",
    "Year",
    "Month",
    "order_date",
    "gender",
    "city",
    "state",
    "loyalty_points"
).agg(
    sum("total_amount").alias("Total_revenue"),
    sum("quantity").alias("Total_quantity"),
    countDistinct("order_id").alias("Total_orders"),
    avg("total_amount").alias("Avg_revenue"),
    avg("quantity").alias("Avg_quantity"),
    avg("loyalty_points").alias("Avg_loyalty_points"),
    min("unit_price").alias("Min_price"),
    max("unit_price").alias("Max_price")
)
gold_df.display()

# store optimized gold tables----------

gold_df.write.format('delta').mode('overwrite').saveAsTable('project.gold.gold_fact_Sales')



