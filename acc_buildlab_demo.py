#glue script to copy s3 data of 3 months 
#update year as per reuqired year
#update month_list variable to value of required months
#for eg if we need data for august,september ,october then month_list=['08','09','10']
#update s3_data_path for the parquet files location
#update s3_output_path for destination where the output of script will reside


#run the script as a AWS Glue job or on AWS Glue jupyter notebook with role having persmission to source and destination s3 bucket



import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,DecimalType,TimestampType,DoubleType
from pyspark.sql.functions import when

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session



month_list=['4']
year='2022'
s3_data_path="s3://d11-athena-billing-hourly-replica/hourly/Cost_usage_hourly_Athena/Cost_usage_hourly_Athena"
#s3_data_path="s3://cur-data-sporta-management-replica/cur-data-sporta-management/cur-data-sporta-management/cur-data-sporta-management"
s3_output_path="s3a://acc-buildapp-test/hourly/Cost_usage_hourly_Athena/Cost_usage_hourly_Athena/"
#s3_output_path="s3a://acc-buildapp-test/cur-data-sporta-management/cur-data-sporta-management/cur-data-sporta-management/"



schema_cur_data=StructType([
StructField("identity_line_item_id",StringType(),True), 
StructField("identity_time_interval",StringType(),True), 
StructField("bill_invoice_id",StringType(),True), 
StructField("bill_billing_entity",StringType(),True), 
StructField("bill_bill_type",StringType(),True), 
StructField("bill_payer_account_id",StringType(),True), 
StructField("bill_billing_period_start_date",TimestampType(),True), 
StructField("bill_billing_period_end_date",TimestampType(),True), 
StructField("line_item_usage_account_id",StringType(),True), 
StructField("line_item_line_item_type",StringType(),True), 
StructField("line_item_usage_start_date",TimestampType(),True), 
StructField("line_item_usage_end_date",TimestampType(),True), 
StructField("line_item_product_code",StringType(),True), 
StructField("line_item_usage_type",StringType(),True), 
StructField("line_item_operation",StringType(),True), 
StructField("line_item_availability_zone",StringType(),True), 
StructField("line_item_resource_id",StringType(),True), 
StructField("line_item_usage_amount",DoubleType(),True), 
StructField("line_item_normalization_factor",DoubleType(),True), 
StructField("line_item_normalized_usage_amount",DoubleType(),True), 
StructField("line_item_currency_code",StringType(),True), 
StructField("line_item_unblended_rate",StringType(),True), 
StructField("line_item_unblended_cost",DoubleType(),True), 
StructField("line_item_blended_rate",StringType(),True), 
StructField("line_item_blended_cost",DoubleType(),True), 
StructField("line_item_line_item_description",StringType(),True), 
StructField("line_item_tax_type",StringType(),True), 
StructField("line_item_legal_entity",StringType(),True), 
StructField("product_product_name",StringType(),True), 
StructField("product_from_location",StringType(),True), 
StructField("product_from_location_type",StringType(),True), 
StructField("product_from_region_code",StringType(),True), 
StructField("product_group",StringType(),True), 
StructField("product_group_description",StringType(),True), 
StructField("product_location",StringType(),True), 
StructField("product_location_type",StringType(),True), 
StructField("product_product_family",StringType(),True), 
StructField("product_region",StringType(),True), 
StructField("product_region_code",StringType(),True), 
StructField("product_Servicecode",StringType(),True), 
StructField("product_Servicename",StringType(),True), 
StructField("product_sku",StringType(),True), 
StructField("product_storage_media",StringType(),True), 
StructField("product_to_location",StringType(),True), 
StructField("product_to_location_type",StringType(),True), 
StructField("product_to_region_code",StringType(),True), 
StructField("product_transfer_type",StringType(),True), 
StructField("line_item_usage_type",StringType(),True), 
StructField("product_version",StringType(),True), 
StructField("pricing_rate_code",StringType(),True), 
StructField("pricing_rate_id",StringType(),True), 
StructField("pricing_currency",StringType(),True), 
StructField("pricing_public_on_demand_cost",DoubleType(),True), 
StructField("pricing_public_on_demand_rate",StringType(),True), 
StructField("pricing_term",StringType(),True), 
StructField("pricing_unit",StringType(),True), 
StructField("reservation_amortized_upfront_cost_for_usage",DoubleType(),True), 
StructField("reservation_amortized_upfront_fee_for_billing_period",DoubleType(),True), 
StructField("reservation_effective_cost",DoubleType(),True), 
StructField("reservation_end_time",StringType(),True), 
StructField("reservation_modification_status",StringType(),True), 
StructField("reservation_normalized_units_per_reservation",StringType(),True), 
StructField("reservation_number_of_reservations",StringType(),True), 
StructField("reservation_recurring_fee_for_usage",DoubleType(),True), 
StructField("reservation_start_time",StringType(),True), 
StructField("reservation_subscription_id",StringType(),True), 
StructField("reservation_total_reserved_normalized_units",StringType(),True), 
StructField("reservation_total_reserved_units",StringType(),True), 
StructField("reservation_units_per_reservation",StringType(),True), 
StructField("reservation_unused_amortized_upfront_fee_for_billing_period",DoubleType(),True), 
StructField("reservation_unused_normalized_unit_quantity",DoubleType(),True), 
StructField("reservation_unused_quantity",DoubleType(),True), 
StructField("reservation_unused_recurring_fee",DoubleType(),True), 
StructField("reservation_upfront_value",DoubleType(),True), 
StructField("savings_plan_total_commitment_to_date",DoubleType(),True), 
StructField("savings_plan_savings_plan_a_r_n",StringType(),True), 
StructField("savings_plan_savings_plan_rate",DoubleType(),True), 
StructField("savings_plan_used_commitment",DoubleType(),True), 
StructField("savings_plan_savings_plan_effective_cost",DoubleType(),True), 
StructField("savings_plan_amortized_upfront_commitment_for_billing_period",DoubleType(),True), 
StructField("savings_plan_recurring_commitment_for_billing_period",DoubleType(),True)
])




for month in month_list:
    cur_data_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema_cur_data)
    print(f"consuming {month} month data")
    key=s3_data_path+"/year="+year+"/month="+month+"/*/"
    
    print(f"key {key}")
    print("done key")
    cur_data_df= spark.read.parquet(key)
    print("sucessfully read data")
    #cur_data_df=cur_data_df.unionByName(temp_df)

    
    



    
    #change line_item_unblended_rate to 0.01
    processed_cur_data_df = cur_data_df \
    .withColumn("line_item_unblended_rate",  when((cur_data_df.line_item_line_item_type == "Refund") \
    & (cur_data_df.line_item_line_item_description == "Enterprise Program Discount") , 0.01) \
    .otherwise(cur_data_df.line_item_unblended_rate))

    #change line_item_unblended_cost to 0.01
    processed_cur_data_df1 = processed_cur_data_df \
    .withColumn("line_item_unblended_cost",  when((cur_data_df.line_item_line_item_type == "Refund") \
    & (cur_data_df.line_item_line_item_description == "Enterprise Program Discount") , 0.01) \
    .otherwise(cur_data_df.line_item_unblended_cost))

    #change line_item_blended_rate to 0.01
    processed_cur_data_df2 = processed_cur_data_df1 \
    .withColumn("line_item_blended_rate",  when((cur_data_df.line_item_line_item_type == "Refund") \
    & (cur_data_df.line_item_line_item_description == "Enterprise Program Discount") , 0.01) \
    .otherwise(cur_data_df.line_item_blended_rate))

    #change line_item_blended_cost to 0.01
    processed_cur_data_df3 = processed_cur_data_df2 \
    .withColumn("line_item_blended_cost",  when((cur_data_df.line_item_line_item_type == "Refund") \
    & (cur_data_df.line_item_line_item_description == "Enterprise Program Discount") , 0.01) \
    .otherwise(cur_data_df.line_item_blended_cost))


    #change line_item_line_item_description to 0.01
    processed_cur_data_df4 = processed_cur_data_df3 \
    .withColumn("line_item_line_item_description",  when((cur_data_df.line_item_line_item_type == "Refund") \
    & (cur_data_df.line_item_line_item_description == "Enterprise Program Discount") , "Masked description") \
    .otherwise(cur_data_df.line_item_line_item_description))



    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_line_item_description",when(cur_data_df.line_item_usage_type.like('%TimedStorage-ByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),"Masked description").otherwise(processed_cur_data_df4.line_item_line_item_description))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_rate",when(cur_data_df.line_item_usage_type.like('%TimedStorage-ByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_cost",when(cur_data_df.line_item_usage_type.like('%TimedStorage-ByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_cost))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_rate",when(cur_data_df.line_item_usage_type.like('%TimedStorage-ByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_cost",when(cur_data_df.line_item_usage_type.like('%TimedStorage-ByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_cost))


    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_line_item_description",when(cur_data_df.line_item_usage_type.like('%TimedStorage-GlacierByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),"Masked description").otherwise(processed_cur_data_df4.line_item_line_item_description))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_rate",when(cur_data_df.line_item_usage_type.like('%TimedStorage-GlacierByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_cost",when(cur_data_df.line_item_usage_type.like('%TimedStorage-GlacierByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_cost))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_rate",when(cur_data_df.line_item_usage_type.like('%TimedStorage-GlacierByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_cost",when(cur_data_df.line_item_usage_type.like('%TimedStorage-GlacierByteHrs%')&(processed_cur_data_df4.product_product_name.like('%Amazon Simple Storage Service%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_cost))

    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_line_item_description",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTP-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),"Masked description").otherwise(processed_cur_data_df4.line_item_line_item_description))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_rate",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTP-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_cost",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTP-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_cost))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_rate",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTP-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_cost",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTP-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_cost))

    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_line_item_description",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTPS-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),"Masked description").otherwise(processed_cur_data_df4.line_item_line_item_description))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_rate",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTPS-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_cost",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTPS-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_cost))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_rate",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTPS-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_cost",when(cur_data_df.line_item_usage_type.like('%-Requests-HTTPS-Proxy')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_cost))


    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_line_item_description",when(cur_data_df.line_item_usage_type.like('%-DataTransfer-Out-Bytes')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),"Masked description").otherwise(processed_cur_data_df4.line_item_line_item_description))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_rate",when(cur_data_df.line_item_usage_type.like('%-DataTransfer-Out-Bytes')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_cost",when(cur_data_df.line_item_usage_type.like('%-DataTransfer-Out-Bytes')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_unblended_cost))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_rate",when(cur_data_df.line_item_usage_type.like('%-DataTransfer-Out-Bytes')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_cost",when(cur_data_df.line_item_usage_type.like('%-DataTransfer-Out-Bytes')&(processed_cur_data_df4.product_product_name.like('%CloudFront%')),0.01).otherwise(processed_cur_data_df4.line_item_blended_cost))

    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_line_item_description",when(cur_data_df.line_item_usage_type.like('%DataTransfer-Regional-Bytes'),"Masked description").otherwise(processed_cur_data_df4.line_item_line_item_description))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_rate",when(cur_data_df.line_item_usage_type.like('%DataTransfer-Regional-Bytes'),0.01).otherwise(processed_cur_data_df4.line_item_unblended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_unblended_cost",when(cur_data_df.line_item_usage_type.like('%DataTransfer-Regional-Bytes'),0.01).otherwise(processed_cur_data_df4.line_item_unblended_cost))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_rate",when(cur_data_df.line_item_usage_type.like('%DataTransfer-Regional-Bytes'),0.01).otherwise(processed_cur_data_df4.line_item_blended_rate))
    processed_cur_data_df4=processed_cur_data_df4.withColumn("line_item_blended_cost",when(cur_data_df.line_item_usage_type.like('%DataTransfer-Regional-Bytes'),0.01).otherwise(processed_cur_data_df4.line_item_blended_cost))

   

    s3_output_path_mod=s3_output_path+"year="+year+"/month="+month+"/"
    processed_cur_data_df4.write.mode('overwrite').parquet(s3_output_path_mod)
    processed_cur_data_df4.unpersist()
    




