import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime,timedelta
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
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_output_path'])


#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) and ('--{}'.format('day') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year','day'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
    args['day'] = args_opt['day']
else:
    logger.info("setting optional parameters")
    previous_date=current_date-timedelta(days=1)
    args['year']=str(previous_date.year)
    args['month']=str(previous_date.month).lstrip("0")
    args['day']=str(previous_date.day)

if len(args['day'])==1:
    args['day']="0"+args['day']   
job.init(args['JOB_NAME'], args)

# args['s3_output_path']='s3://acc-buildapp-test/processed/ec2_usage_insights_fact/'
# args['s3_temp_path']='s3://acc-buildapp-test/acc_build_lab_temp'
# args['full_load']='false'

args['raw_table']='raw_cloudwatch'
args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'

args['processed_database']='d11_cost_insights_v2'
#args['processed_database']='build_lab'
args['processed_table']='processed_resource_id_to_tags_mapping'


    
def getValues(rec):
    rec['timestamp']=rec['timestamp']/1000
    rec['value_min']=rec['value']['min']
    rec['value_max']=rec['value']['max']
    rec['value_sum']=rec['value']['sum']
    rec['value_count']=rec['value']['count']
    
    del rec['value']
    
    
    
    
    return rec




logger.info("****using following args****")
logger.info(str(args))
logger.info("****using following args****")

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = args['processed_database'], table_name = args['processed_table'], transformation_ctx = "datasource1")
mapping_df_raw=datasource1.toDF()

mapping_df_raw.createOrReplaceTempView("resource_id_to_tags_mapping")

mapping_df=spark.sql(''' 
            SELECT line_item_resource_id,tag_dim_id,line_item_usage_account_id
            FROM
            (
                SELECT t.*, ROW_NUMBER() OVER (PARTITION BY line_item_resource_id ORDER BY line_item_usage_start_date DESC) rn
                FROM resource_id_to_tags_mapping  t
            ) t
            WHERE rn = 1
            ''')
            
for day in range(1,29):
    args['day']=str(day) 
    if len(args['day'])==1:
        args['day']="0"+args['day']
    print(f"processing for args {args}")
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['raw_table'], transformation_ctx = "datasource0", push_down_predicate = "(namespace == 'ELB'  and month=='{}' and year=='{}' and day=='{}')".format(args['month'],args['year'],args['day']))
    
    mapped_dyF =  Map.apply(frame = datasource0, f = getValues)
    
    
    filter_dyF = Filter.apply(frame = mapped_dyF, f = lambda x: x["metric_name"] in ["ConsumedLCUs", "PreWarmedLCUs"])
    
    
    df = filter_dyF.toDF()
    
    
    df = (df.withColumn("metric_ts", col("timestamp").cast("timestamp")).withColumn("metric_date", col("metric_ts").cast("date"))
      .withColumn("metric_hour", hour(col("metric_ts")))).drop("timestamp","metric_stream_name")
      
    
    df = df.withColumn("load_balancer_name",get_json_object(col("dimensions"),"$.LoadBalancerName").alias("load_balancer_name")).drop("dimensions")
    df.createOrReplaceTempView("elb_cloudwatch")
    
    cons_lcu_df=spark.sql(''' 
    select load_balancer_name,metric_date,account_id,
    
    sum(value_sum) as consumed_lcus 
    from elb_cloudwatch where metric_name=='ConsumedLCUs' group by load_balancer_name,metric_date,account_id
    
    ''')
    
    pre_warmed_lcus_df=spark.sql(''' 
    select load_balancer_name,metric_date,account_id,
    
    sum(value_sum) as pre_warmed_lcus 
     from elb_cloudwatch where metric_name=='PreWarmedLCUs' group by load_balancer_name,metric_date,account_id
    
    ''')
    
    df_joined_1=cons_lcu_df.join(pre_warmed_lcus_df,[cons_lcu_df.load_balancer_name==pre_warmed_lcus_df.load_balancer_name,cons_lcu_df.metric_date==pre_warmed_lcus_df.metric_date]).select(cons_lcu_df.load_balancer_name,cons_lcu_df.metric_date,cons_lcu_df.account_id,cons_lcu_df.consumed_lcus,pre_warmed_lcus_df.pre_warmed_lcus)
    df_joined_3=df_joined_1.withColumn("year",year(col("metric_date"))).withColumn("month",month(col("metric_date"))).withColumn("day",dayofmonth(col("metric_date"))).withColumn("date_dim_id",date_format(col("metric_date"),"yyyyMMdd"))
    
    df_joined_3=df_joined_3.join(mapping_df,[df_joined_3.load_balancer_name==mapping_df.line_item_resource_id,df_joined_3.account_id==mapping_df.line_item_usage_account_id])
    df_joined_3=df_joined_3.drop("line_item_resource_id","line_item_usage_account_id","metric_date").withColumnRenamed("load_balancer_name","resource").withColumnRenamed("account_id","aws_account_dim_id")
    
    df_joined_3.write.mode("append").partitionBy("year","month","day").parquet(args['s3_output_path'])
    job.commit()

#run on based on job bookmarks and partition job as well/no push down predicate
logger.info("***succesfull execution**")