
#--additional-python-modules pyarrow==2,awswrangler==2.4.0
import sys
import awswrangler as wr
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
logger = glueContext.get_logger()
tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)

#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_output_path','full_load','s3_temp_path'])

#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    tz=pytz.timezone('Asia/Calcutta')
    current_date=datetime.now(tz)
    logger.info("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)

job.init(args['JOB_NAME'], args)  
# args['s3_output_path']='s3://acc-buildapp-test/processed/cost_insights_fact/'
# args['s3_temp_path']='s3://acc-buildapp-test/acc_build_lab_temp'
# args['full_load']='false'

args['s3_temp_path_sec']=args['s3_temp_path']+"/second_summary_view"
args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'

def generate_sha(str):
    return sha2(str,256)
    


logger.info("****using following args****")
logger.info(str(args))
logger.info("****using following args****")

#clear s3
wr.s3.delete_objects(args['s3_temp_path'])

athena_view_dataframe = (
    glueContext.read.format("jdbc")
    .option("mode","overwrite")
    .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
    .option("driver", "com.simba.athena.jdbc.Driver")
    .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
    .option("dbtable", f"{args['raw_database']}.summary_view")
    .option("S3OutputLocation",args['s3_temp_path']) # CSVs/metadata dumped here on load
    .load()
    )

athena_view_dataframe_sec = (
    glueContext.read.format("jdbc")
    .option("mode","overwrite")
    .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
    .option("driver", "com.simba.athena.jdbc.Driver")
    .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
    .option("dbtable", f"{args['raw_database']}.summary_view_second")
    .option("S3OutputLocation",args['s3_temp_path_sec']) # CSVs/metadata dumped here on load
    .load()
    )



athena_view_datasource = DynamicFrame.fromDF(athena_view_dataframe, glueContext, "athena_view_datasource")
athena_view_datasource_df=athena_view_datasource.toDF()

print("athena_view_datasource_df:",athena_view_datasource_df.count())

athena_view_datasource_sec = DynamicFrame.fromDF(athena_view_dataframe_sec, glueContext, "athena_view_datasource_sec")
athena_view_datasource_df_sec=athena_view_datasource_sec.toDF()
print("athena_view_datasource_df_sec:",athena_view_datasource_df_sec.count())

athena_view_datasource_df=athena_view_datasource_df.unionByName(athena_view_datasource_df_sec, allowMissingColumns=True)
print("merged ",athena_view_datasource_df.count())


column_list=athena_view_datasource_df.columns
filtered_list=[]


filtered_list=[ s for s in column_list if s.startswith('resource_tags_user_')]
filtered_list.sort()
filtered_list_1=[ "Coalesce(nullif("+s+",''),'') as "+s for s in filtered_list ]

colnm_str=','.join(filtered_list)
colnm_str_1=','.join(filtered_list_1)

print("colnm_str:",colnm_str)


resource_dim_column_list=['line_item_operation', 'line_item_product_code', 'product_product_family', 'product_region', 'product_servicecode', 'product_servicename']
resource_dim_column_list.sort()
colnm_str_resource=','.join(resource_dim_column_list)

athena_view_datasource_df.createOrReplaceTempView("summary_view")



if args['full_load']=='false':
    cost_insights_df=spark.sql(''' SELECT 
    
    linked_account_id as aws_account_dim_id,
    date_format(cast(usage_date as date),'yyyyMMdd')  as date_dim_id, 
            {},
            charge_type,
            purchase_option,
            charge_category,
            product_code,
            {},
            
            sum(amortized_cost) AS amortized_cost,
            
            sum(unblended_cost) AS unblended_cost,
            sum(ri_sp_trueup) AS ri_sp_trueup,
            sum(ri_sp_upfront_fees) AS ri_sp_upfront_fees,
            sum(public_cost) AS public_cost,
            
            year,
            month
            FROM summary_view
            WHERE month='{}' and year='{}'
            GROUP BY  linked_account_id,usage_date,{},charge_type,purchase_option,charge_category,product_code,{},year,
            month '''.format(colnm_str_resource,colnm_str_1,args['month'],args['year'],colnm_str_resource,colnm_str))
else:
    cost_insights_df=spark.sql(''' SELECT 
    
    linked_account_id as aws_account_dim_id,
    date_format(cast(usage_date as date),'yyyyMMdd')  as date_dim_id, 
            {},
            charge_type,
            purchase_option,
            charge_category,
            product_code,
            {},
            
            sum(amortized_cost) AS amortized_cost,
            
            sum(unblended_cost) AS unblended_cost,
            sum(ri_sp_trueup) AS ri_sp_trueup,
            sum(ri_sp_upfront_fees) AS ri_sp_upfront_fees,
            sum(public_cost) AS public_cost,
            year,
            month
            FROM summary_view
            
            GROUP BY  linked_account_id,usage_date,{},charge_type,purchase_option,charge_category,product_code,{},year,
            month '''.format(colnm_str_resource,colnm_str_1,colnm_str_resource,colnm_str))


cost_insights_df=cost_insights_df.withColumn('concated_tags',concat_ws("||",*filtered_list))

cost_insights_df=cost_insights_df.withColumn('tag_dim_id',generate_sha(cost_insights_df.concated_tags))


cost_insights_df=cost_insights_df.drop("concated_tags")
cost_insights_df=cost_insights_df.drop(*filtered_list)



cost_insights_df=cost_insights_df.withColumn('concated_tags_resource',concat_ws("||",*resource_dim_column_list))

cost_insights_df=cost_insights_df.withColumn('aws_product_dim_id',generate_sha(cost_insights_df.concated_tags_resource))


cost_insights_df=cost_insights_df.drop("concated_tags_resource")
cost_insights_df=cost_insights_df.drop(*resource_dim_column_list)


cost_insights_df.repartition("date_dim_id").write.mode("overwrite").partitionBy("year","month").parquet(args['s3_output_path'])
job.commit()











