
#--write-shuffle-spills-to-s3 true #memory issue
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
tz=pytz.timezone('Asia/Calcutta')

current_date=datetime.datetime.now(tz)


#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','full_load','s3_output_path'])

#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    print("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)

spark = SparkSession.builder.config("spark.sql.sources.partitionOverwriteMode","dynamic").config("spark.sql.parquet.enableVectorizedReader","false").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

def generate_sha(str):
    
    return sha2(str,256)


def stripData(val):
    val=str(val)
    slash_pos=(val).rfind("/")
    col_pos=val.rfind(":")
    
    if slash_pos>col_pos:
        to_split=slash_pos
    else:
        to_split=col_pos
    
    fin_val=val[to_split+1:]
    
   
    return fin_val
    






args['raw_table']='raw_cost_usage_hourly_athena'
args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'


datasource0 = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['raw_table'], transformation_ctx = "datasource0",additional_options={"mergeSchema": "true"} )


df=datasource0.toDF()


column_list=df.columns

filtered_list=[]


filtered_list=[ s for s in column_list if s.startswith('resource_tags_user_')]
filtered_list.sort()


#colnm_list_1=[ "Coalesce(nullif("+s+",''),'') as "+s for s in filtered_list ]

colnm_str=','.join(filtered_list)
#colnm_str=','.join(colnm_list_1)

df.createOrReplaceTempView("raw_cost_usage_hourly_athena")


if args['full_load']=='false':
    
    spark_df=spark.sql(''' SELECT line_item_resource_id,line_item_usage_start_date ,line_item_usage_account_id,{} ,
                            date_format(line_item_usage_start_date ,'M') as month,
                             date_format(line_item_usage_start_date ,'y') as year,
                              date_format(line_item_usage_start_date ,'d') as day
                    FROM
                    (
                        SELECT t.*, ROW_NUMBER() OVER (PARTITION BY line_item_resource_id ORDER BY line_item_usage_start_date DESC) rn
                        FROM raw_cost_usage_hourly_athena t
                    ) t
                    WHERE rn = 1 and month='{}' and year='{}' 
                '''.format(colnm_str,args['month'],args['year']))
   
    
else:
    spark_df=spark.sql(''' SELECT line_item_resource_id,line_item_usage_start_date ,line_item_usage_account_id,{},
                             date_format(line_item_usage_start_date ,'M') as month,
                             date_format(line_item_usage_start_date ,'y') as year,
                             date_format(line_item_usage_start_date ,'d') as day
                                    FROM
                                    (
                                        SELECT t.*, ROW_NUMBER() OVER (PARTITION BY line_item_resource_id ORDER BY line_item_usage_start_date DESC) rn
                                        FROM raw_cost_usage_hourly_athena t
                                    ) t
                                    WHERE rn = 1 
                        '''.format(colnm_str))
                           
spark_df=spark_df.na.fill("")
usage_insights_df=spark_df.withColumn('concated_tags',concat_ws("||",*filtered_list))

usage_insights_df=usage_insights_df.withColumn('tag_dim_id',generate_sha(usage_insights_df.concated_tags))

usage_insights_df=usage_insights_df.drop("concated_tags")
usage_insights_df=usage_insights_df.drop(*filtered_list)


stripDataUDF = udf(lambda z:stripData(z),StringType())

usage_insights_df=usage_insights_df.withColumn("line_item_resource_id", stripDataUDF(col("line_item_resource_id")))

usage_insights_df.repartition("day").write.mode("overwrite").partitionBy("year","month","day").parquet(args['s3_output_path'])
job.commit()
logger.info("***succesfull execution**")


