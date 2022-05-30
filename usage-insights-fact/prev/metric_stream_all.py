import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import ast
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_input_path','s3_output_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
# args['s3_input_path']='s3://metricstreams-cloudwatch-metrics-izeprtvp-replica/'
# args['s3_output_path']='s3://acc-buildapp-test/raw/cloudwatch'
def renameNamespace(rec):
    rec['namespace'] = rec['namespace'].replace('/', '')
    
    return rec
    


all_exclusion=['s3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2021/09/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2021/11/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2021/10/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2021/12/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/01/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/03/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/01/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/02/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/03/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/04/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/05/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/06/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/07/**',
 's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/08/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/09/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/10/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/11/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/12/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/13/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/14/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/15/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/16/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/17/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/18/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/19/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/20/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/21/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/22/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/23/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/24/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/25/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/26/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/27/**',
's3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/28/**'
]
 
for day_ in range(1,29):
    day_=str(day_)
    if len(day_)==1:
        day_="0"+day_
    print(f"processing for {day_}")
    included_path="s3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/"+day_+"/**"
    args["s3_input_path"]="s3://metricstreams-cloudwatch-metrics-izeprtvp-replica/2022/02/"+day_+"/"
    all_exclusion.remove(included_path)

    datasource0 = glueContext.create_dynamic_frame.from_options(connection_type="s3", connection_options = {"paths": [args["s3_input_path"]],"recurse":True,"groupFiles":"inPartition","compressionType":"snappy","exclusions":all_exclusion},format="json", transformation_ctx="datasource0")
    
    applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("metric_stream_name", "string", "metric_stream_name", "string"), ("account_id", "string", "account_id", "string"), ("region", "string", "region", "string"), ("namespace", "string", "namespace", "string"), ("metric_name", "string", "metric_name", "string"), ("dimensions", "struct", "dimensions", "string"), ("timestamp", "long", "timestamp", "long"), ("value", "struct", "value", "struct"), ("unit", "string", "unit", "string")], transformation_ctx = "applymapping1")
    df=applymapping1.toDF()
    
    
    df = df.withColumn("metric_ts", (df.timestamp/1000).cast("timestamp")).withColumn("year", date_format(col("metric_ts"), "yyyy")).withColumn("month", date_format(col("metric_ts"), "MM")).withColumn("day", date_format(col("metric_ts"), "dd")).withColumn("hour", date_format(col("metric_ts"), "HH")).drop("metric_ts")
    applymapping1 = DynamicFrame.fromDF(df, glueContext, "applymapping1")
    
    resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
    dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
    mapped_dyF =  Map.apply(frame = dropnullfields3, f = renameNamespace)
    datasink4 = glueContext.write_dynamic_frame.from_options(frame = mapped_dyF, connection_type = "s3", connection_options = {"path":args['s3_output_path'] ,"partitionKeys": ["namespace", "year", "month","day", "hour"]}, format = "parquet", transformation_ctx = "datasink4")
    all_exclusion.append(included_path)
    job.commit()
    logger.info("***succesfull execution**")



