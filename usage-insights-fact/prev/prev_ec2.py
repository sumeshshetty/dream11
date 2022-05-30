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


    
def extractInstanceId(rec):
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
    datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['raw_table'], transformation_ctx = "datasource0", push_down_predicate = "(namespace == 'AWSEC2'  and month=='{}' and year=='{}' and day=='{}')".format(args['month'],args['year'],args['day']))
    
    mapped_dyF =  Map.apply(frame = datasource0, f = extractInstanceId)
    
    
    filter_dyF = Filter.apply(frame = mapped_dyF, f = lambda x: x["metric_name"] in ["CPUUtilization", "NetworkIn", "NetworkOut"])
    
    
    df = filter_dyF.toDF()
    
    df = (df.withColumn("metric_ts", col("timestamp").cast("timestamp")).withColumn("metric_date", col("metric_ts").cast("date"))
      .withColumn("metric_hour", hour(col("metric_ts")))).drop("timestamp","metric_stream_name")
      
    
    df = df.withColumn("instance_id",get_json_object(col("dimensions"),"$.InstanceId").alias("instance_id")).drop("dimensions")
    df.createOrReplaceTempView("ec2_cloudwatch")
    
    cpu_df_1=spark.sql(''' 
    select instance_id,metric_date,account_id,
    
    percentile(value_sum/value_count, 0.95) as p95_cpu ,
    percentile(value_sum/value_count, 0.99) as p99_cpu,
    percentile(value_sum/value_count, 0.90) as p90_cpu,
    percentile(value_sum/value_count, 0.50) as p50_cpu,
    avg(value_sum/value_count) as avg_cpu 
    
    from ec2_cloudwatch where metric_name=='CPUUtilization' group by instance_id,metric_date,account_id
    
    ''')
    
    
    cpu_df_2=spark.sql(''' 
    select instance_id, account_id, metric_date, max(val) as max_p99 
    from (select instance_id, account_id, metric_date, metric_hour, (percentile(value_sum/value_count, 0.99)) as val from ec2_cloudwatch 
    where metric_name=='CPUUtilization' group by instance_id, account_id, metric_date, metric_hour) 
    group by instance_id, account_id, metric_date 
    ''')
    
    
    
    
    cpu_df=cpu_df_1.join(cpu_df_2,[cpu_df_1.instance_id==cpu_df_2.instance_id,cpu_df_1.metric_date==cpu_df_2.metric_date,cpu_df_1.account_id==cpu_df_2.account_id]).select(cpu_df_1.instance_id,cpu_df_1.metric_date,cpu_df_1.account_id,cpu_df_1.p95_cpu,cpu_df_1.p99_cpu,cpu_df_1.p90_cpu,cpu_df_1.p50_cpu,cpu_df_1.avg_cpu,cpu_df_2.max_p99)

    
    net_in_df=spark.sql(''' 
    select instance_id,metric_date,account_id,
    
    sum(value_sum) as network_in 
    from ec2_cloudwatch where metric_name=='NetworkIn' group by instance_id,metric_date,account_id
    
    ''')
    
    net_out_df=spark.sql(''' 
    select instance_id,metric_date,account_id,
    
    sum(value_sum) as network_out 
    from ec2_cloudwatch where metric_name=='NetworkOut' group by instance_id,metric_date,account_id
    
    ''')
    
    df_joined_1=cpu_df.join(net_in_df,[cpu_df.instance_id==net_in_df.instance_id,cpu_df.metric_date==net_in_df.metric_date]).select(cpu_df.instance_id,cpu_df.metric_date,cpu_df.account_id,cpu_df.p95_cpu,cpu_df.p99_cpu,cpu_df.avg_cpu,net_in_df.network_in)
    df_joined_2=df_joined_1.join(net_out_df,[df_joined_1.instance_id==net_out_df.instance_id,df_joined_1.metric_date==net_out_df.metric_date]).select(df_joined_1.instance_id,df_joined_1.metric_date,df_joined_1.account_id,df_joined_1.p95_cpu,df_joined_1.p99_cpu,df_joined_1.avg_cpu,df_joined_1.network_in,net_out_df.network_out)
    
    df_joined_3=df_joined_2.withColumn("year",year(col("metric_date"))).withColumn("month",month(col("metric_date"))).withColumn("day",dayofmonth(col("metric_date"))).withColumn("date_dim_id",date_format(col("metric_date"),"yyyyMMdd"))
    df_joined_3=df_joined_3.join(mapping_df,[df_joined_3.instance_id==mapping_df.line_item_resource_id,df_joined_3.account_id==mapping_df.line_item_usage_account_id])
    
    df_joined_3=df_joined_3.drop("line_item_resource_id","line_item_usage_account_id","metric_date").withColumnRenamed("instance_id","resource").withColumnRenamed("account_id","aws_account_dim_id")
    
    df_joined_3.write.mode("append").partitionBy("year","month","day").parquet(args['s3_output_path'])
    job.commit()

#run on based on job bookmarks and partition job as well/no push down predicate
logger.info("***succesfull execution**")