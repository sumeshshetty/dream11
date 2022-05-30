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
    args['month']=str(previous_date.month)
    args['day']=str(previous_date.day)
if len(args['day'])==1:
    args['day']="0"+args['day']   

if len(args['month'])==1:
    args['month']="0"+args['month']
job.init(args['JOB_NAME'], args)

# args['s3_output_path']='s3://acc-buildapp-test/processed/ec2_usage_insights_fact/'
# args['s3_temp_path']='s3://acc-buildapp-test/acc_build_lab_temp'
# args['full_load']='false'

args['raw_table']='raw_cloudwatch'
# args['raw_database']='d11_cost_insights_v2'
args['raw_database']='build_lab'

# args['processed_database']='d11_cost_insights_v2'
args['processed_database']='build_lab'
args['processed_table']='processed_resource_id_to_tags_mapping'


    
def getValues(rec):
    rec['timestamp']=rec['timestamp']/1000
    rec['value_min']=rec['value']['min']
    rec['value_max']=rec['value']['max']
    rec['value_sum']=rec['value']['sum']
    rec['value_count']=rec['value']['count']
    
    del rec['value']
    
    # rec['instance_id'] = rec['dimensions']['InstanceId']
    # del rec['dimensions']
    
    
    return rec

# try:
#     args['month']='10'
#     args['year']='2021'
#     args['day']='11'
# except Exception :
    
#     #args['month']=str(current_date.month).lstrip("0")
#     #args['year']=str(current_date.year)
#     previous_date=current_date-timedelta(days=1)
#     args['year']=str(previous_date.year)
#     args['month']=str(previous_date.month).lstrip("0")
#     args['day']=str(previous_date.day)


logger.info("****using following args****")
logger.info(str(args))
logger.info("****using following args****")
#psuhdown prede to be replaced by previous dates
#job will run on daily basis
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['raw_table'], transformation_ctx = "datasource0", push_down_predicate = "(namespace == 'AWSApplicationELB'  and month=='{}' and year=='{}' and day=='{}')".format(args['month'],args['year'],args['day']))

mapped_dyF =  Map.apply(frame = datasource0, f = getValues)


filter_dyF = Filter.apply(frame = mapped_dyF, f = lambda x: x["metric_name"] in ["HealthyHostCount", "RequestCount", "ConsumedLCUs"])


df = filter_dyF.toDF()
#df.show()
df = (df.withColumn("metric_ts", col("timestamp").cast("timestamp")).withColumn("metric_date", col("metric_ts").cast("date"))
  .withColumn("metric_hour", hour(col("metric_ts")))).drop("timestamp","metric_stream_name")
  

df = df.withColumn("load_balancer_id",get_json_object(col("dimensions"),"$.LoadBalancer").alias("load_balancer_id")).drop("dimensions")
df.createOrReplaceTempView("alb_cloudwatch")


hhc_df=spark.sql(''' 
select load_balancer_id,metric_date,account_id,

sum(value_sum) as healthy_host_count 
 from alb_cloudwatch where metric_name=='HealthyHostCount' group by load_balancer_id,metric_date,account_id

''')

req_count_df=spark.sql(''' 
select load_balancer_id,metric_date,account_id,

sum(value_sum) as request_count 
from alb_cloudwatch where metric_name=='RequestCount' group by load_balancer_id,metric_date,account_id

''')
#net_in_df.show()

cons_lcu_df=spark.sql(''' 
select load_balancer_id,metric_date,account_id,

sum(value_sum) as consumed_lcus 
from alb_cloudwatch where metric_name=='ConsumedLCUs' group by load_balancer_id,metric_date,account_id

''')
#net_out_df.show()


#df_joined_1=cpu_df.join(net_in_df,[cpu_df.instance_id==net_in_df.instance_id,cpu_df.metric_date==net_in_df.metric_date]).select(cpu_df.instance_id,cpu_df.metric_date,cpu_df.account_id,cpu_df.p95_cpu,cpu_df.p99_cpu,cpu_df.avg_cpu,net_in_df.network_in)
df_joined_1=hhc_df.join(req_count_df,[hhc_df.load_balancer_id==req_count_df.load_balancer_id,hhc_df.metric_date==req_count_df.metric_date]).select(hhc_df.load_balancer_id,hhc_df.metric_date,hhc_df.account_id,hhc_df.healthy_host_count,req_count_df.request_count)

#df_joined_2=df_joined_1.join(net_out_df,[df_joined_1.instance_id==net_out_df.instance_id,df_joined_1.metric_date==net_out_df.metric_date]).select(df_joined_1.instance_id,df_joined_1.metric_date,df_joined_1.account_id,df_joined_1.p95_cpu,df_joined_1.p99_cpu,df_joined_1.avg_cpu,df_joined_1.network_in,net_out_df.network_out)
df_joined_2=df_joined_1.join(cons_lcu_df,[df_joined_1.load_balancer_id==cons_lcu_df.load_balancer_id,df_joined_1.metric_date==cons_lcu_df.metric_date]).select(df_joined_1.load_balancer_id,df_joined_1.metric_date,df_joined_1.account_id,df_joined_1.healthy_host_count,df_joined_1.request_count,cons_lcu_df.consumed_lcus)




df_joined_3=df_joined_2.withColumn("year",year(col("metric_date"))).withColumn("month",month(col("metric_date"))).withColumn("day",dayofmonth(col("metric_date"))).withColumn("date_dim_id",date_format(col("metric_date"),"yyyyMMdd"))

print("########### df_joined_3  ##############")
df_joined_3.show()
print("########### df_joined_3  ##############")

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


print("########### mapping_df  ##############")
mapping_df.show()
print("########### mapping_df  ##############")

df_joined_3=df_joined_3.join(mapping_df,[df_joined_3.load_balancer_id==mapping_df.line_item_resource_id,df_joined_3.account_id==mapping_df.line_item_usage_account_id])


print("########### after join df_joined_3  ##############")
df_joined_3.show()
print("########### after join df_joined_3  ##############")

df_joined_3=df_joined_3.drop("line_item_resource_id","line_item_usage_account_id","metric_date").withColumnRenamed("load_balancer_id","resource").withColumnRenamed("account_id","aws_account_dim_id")

df_joined_3.write.mode("append").partitionBy("year","month","day").parquet(args['s3_output_path'])
job.commit()

#run on based on job bookmarks and partition job as well/no push down predicate
logger.info("***succesfull execution**")