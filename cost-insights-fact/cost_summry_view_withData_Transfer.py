
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


args['dt_s3_output_path']=str(args['s3_output_path']).replace("cost_insights_fact","dt_insights_fact")
athena_view_datasource = DynamicFrame.fromDF(athena_view_dataframe, glueContext, "athena_view_source")
athena_view_datasource_df=athena_view_datasource.toDF()


column_list=athena_view_datasource_df.columns
filtered_list=[]


filtered_list=[ s for s in column_list if s.startswith('resource_tags_user_')]
filtered_list.sort()
colnm_str=','.join(filtered_list)

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
            month '''.format(colnm_str_resource,colnm_str,args['month'],args['year'],colnm_str_resource,colnm_str))
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
            month '''.format(colnm_str_resource,colnm_str,colnm_str_resource,colnm_str))

    dt_df=spark.sql(''' select  
case
when line_item_product_code='AmazonApiGateway' then 'Amazon API Gateway'
when line_item_product_code='AmazonAthena' then 'Amazon Athena'
when line_item_product_code='AmazonCloudFront' then 'Amazon CloudFront'
when line_item_product_code='AmazonCloudWatch' then 'AmazonCloudWatch'
when line_item_product_code='AmazonCognito' then 'Amazon Cognito'
when line_item_product_code='AmazonDocDB' then 'Amazon DocumentDB (with MongoDB compatibility)'
when line_item_product_code='AmazonDynamoDB' then 'Amazon DynamoDB'
when line_item_product_code='AmazonEC2' then 'Amazon Elastic Compute Cloud'
when line_item_product_code='AmazonECR' then 'Amazon EC2 Container Registry (ECR)'
when line_item_product_code='AmazonECRPublic' then 'Amazon Elastic Container Registry Public'
when line_item_product_code='AmazonECS' then 'Amazon Elastic Container Service'
when line_item_product_code='AmazonEFS' then 'Amazon Elastic File System'
when line_item_product_code='AmazonEKS' then 'Amazon Elastic Container Service for Kubernetes'
when line_item_product_code='AmazonElastiCache' then 'Amazon ElastiCache'
when line_item_product_code='AmazonES' then 'Amazon Elasticsearch Service'
when line_item_product_code='AmazonForecast' then 'Amazon Forecast'
when line_item_product_code='AmazonGlacier' then ''
when line_item_product_code='AmazonGuardDuty' then 'Amazon GuardDuty'
when line_item_product_code='AmazonInspector' then 'Amazon Inspector'
when line_item_product_code='AmazonKinesis' then 'Amazon Kinesis'
when line_item_product_code='AmazonKinesisAnalytics' then 'Amazon Kinesis Analytics'
when line_item_product_code='AmazonKinesisFirehose' then 'Amazon Kinesis Firehose'
when line_item_product_code='AmazonLightsail' then 'Amazon Lightsail'
when line_item_product_code='AmazonMCS' then ''
when line_item_product_code='AmazonMQ' then ''
when line_item_product_code='AmazonMSK' then 'Amazon Managed Streaming for Apache Kafka'
when line_item_product_code='AmazonMWAA' then 'Amazon Managed Workflows for Apache Airflow'
when line_item_product_code='AmazonNeptune' then 'Amazon Neptune'
when line_item_product_code='AmazonPersonalize' then 'Amazon Personalize'
when line_item_product_code='AmazonPolly' then 'Amazon Polly'
when line_item_product_code='AmazonQuickSight' then 'Amazon QuickSight'
when line_item_product_code='AmazonRDS' then 'Amazon Relational Database Service'
when line_item_product_code='AmazonRedshift' then 'Amazon Redshift'
when line_item_product_code='AmazonRekognition' then 'Amazon Rekognition'
when line_item_product_code='AmazonRoute53' then 'Amazon Route 53'
when line_item_product_code='AmazonS3' then 'Amazon Simple Storage Service'
when line_item_product_code='AmazonS3GlacierDeepArchive' then 'Amazon S3 Glacier Deep Archive'
when line_item_product_code='AmazonSageMaker' then 'Amazon SageMaker'
when line_item_product_code='AmazonSES' then 'Amazon Simple Email Service'
when line_item_product_code='AmazonSimpleDB' then 'Amazon SimpleDB'
when line_item_product_code='AmazonSNS' then 'Amazon Simple Notification Service'
when line_item_product_code='AmazonStates' then ''
when line_item_product_code='AmazonTimestream' then 'Amazon Timestream'
when line_item_product_code='AmazonVPC' then 'Amazon Virtual Private Cloud'
when line_item_product_code='AmazonWorkDocs' then 'Amazon WorkDocs'
when line_item_product_code='AmazonWorkSpaces' then 'Amazon WorkSpaces'
when line_item_product_code='AppFlow' then 'Amazon AppFlow'
when line_item_product_code='AWSAmplify' then 'AWS Amplify'
when line_item_product_code='AWSAppSync' then 'AWS AppSync'
when line_item_product_code='AWSBackup' then 'AWS Backup'
when line_item_product_code='AWSBudgets' then 'AWS Budgets'
when line_item_product_code='AWSCertificateManager' then 'AWS Certificate Manager'
when line_item_product_code='AWSCloudShell' then 'AWS CloudShell'
when line_item_product_code='AWSCloudTrail' then 'AWS CloudTrail'
when line_item_product_code='AWSCodeArtifact' then 'AWS CodeArtifact'
when line_item_product_code='AWSCodeCommit' then 'AWS CodeCommit'
when line_item_product_code='AWSCodePipeline' then 'AWS CodePipeline'
when line_item_product_code='AWSConfig' then 'AWS Config'
when line_item_product_code='AWSCostExplorer' then 'AWS Cost Explorer'
when line_item_product_code='AWSDatabaseMigrationSvc' then 'AWS Database Migration Service'
when line_item_product_code='AWSDataTransfer' then 'AWS Data Transfer'
when line_item_product_code='AWSDirectoryService' then 'AWS Directory Service'
when line_item_product_code='AWSELB' then 'Elastic Load Balancing'
when line_item_product_code='AWSElementalMediaConvert' then 'AWS Elemental MediaConvert'
when line_item_product_code='AWSElementalMediaLive' then 'AWS Elemental MediaLive'
when line_item_product_code='AWSElementalMediaPackage' then 'AWS Elemental MediaPackage'
when line_item_product_code='AWSElementalMediaStore' then ''
when line_item_product_code='AWSElementalMediaTailor' then 'AWS Elemental MediaTailor'
when line_item_product_code='AWSEvents' then ''
when line_item_product_code='AWSGlobalAccelerator' then 'AWS Global Accelerator'
when line_item_product_code='AWSGlue' then 'AWS Glue'
when line_item_product_code='awskms' then 'AWS Key Management Service'
when line_item_product_code='AWSLambda' then 'AWS Lambda'
when line_item_product_code='AWSQueueService' then 'Amazon Simple Queue Service'
when line_item_product_code='AWSSecretsManager' then 'AWS Secrets Manager'
when line_item_product_code='AWSSecurityHub' then 'AWS Security Hub'
when line_item_product_code='AWSShield' then 'AWS Shield'
when line_item_product_code='AWSSystemsManager' then 'AWS Systems Manager'
when line_item_product_code='AWSTransfer' then 'AWS Transfer Family'
when line_item_product_code='awswaf' then 'AWS WAF'
when line_item_product_code='AWSXRay' then 'AWS X-Ray'
when line_item_product_code='CodeBuild' then ''
when line_item_product_code='comprehend' then 'Amazon Comprehend'
when line_item_product_code='ComputeSavingsPlans' then 'Savings Plans for AWS Compute usage'
when line_item_product_code='datapipeline' then 'AWS Data Pipeline'
when line_item_product_code='ElasticMapReduce' then 'Amazon Elastic MapReduce'
when line_item_product_code='OCBElasticComputeCloud' then ''
when line_item_product_code='OCBPremiumSupport' then ''
when line_item_product_code='translate' then 'Amazon Translate'

else ''
END as product_servicename
,line_item_product_code,
linked_account_id,month,year, 
sum(
CASE
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonApiGateway') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonAthena') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonCloudFront') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonCloudWatch') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonCognito') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonDocDB') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonDynamoDB') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonEC2') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonECR') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonECRPublic') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonECS') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonEFS') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonEKS') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonElastiCache') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonES') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonForecast') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonGlacier') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonGuardDuty') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonInspector') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonKinesis') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonKinesisAnalytics') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonKinesisFirehose') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonLightsail') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonMCS') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonMQ') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonMSK') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonMWAA') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonNeptune') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonPersonalize') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonPolly') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonQuickSight') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonRDS') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonRedshift') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonRekognition') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonRoute53') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonS3') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonS3GlacierDeepArchive') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonSageMaker') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonSES') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonSimpleDB') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonSNS') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonStates') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonTimestream') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonVPC') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonWorkDocs') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AmazonWorkSpaces') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AppFlow') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSAmplify') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSAppSync') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSBackup') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSBudgets') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCertificateManager') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCloudShell') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCloudTrail') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCodeArtifact') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCodeCommit') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCodePipeline') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSConfig') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSCostExplorer') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSDatabaseMigrationSvc') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSDataTransfer') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSDirectoryService') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSELB') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSElementalMediaConvert') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSElementalMediaLive') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSElementalMediaPackage') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSElementalMediaStore') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSElementalMediaTailor') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSEvents') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSGlobalAccelerator') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSGlue') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='awskms') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSLambda') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSQueueService') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSSecretsManager') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSSecurityHub') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSShield') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSSystemsManager') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSTransfer') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='awswaf') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='AWSXRay') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='CodeBuild') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='comprehend') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='ComputeSavingsPlans') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='datapipeline') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='ElasticMapReduce') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='OCBElasticComputeCloud') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='OCBPremiumSupport') then amortized_cost
when( product_servicename='AWS Data Transfer'  and line_item_product_code='translate') then amortized_cost

else 0
  
END) as DT_cost
from summary_view   


group by line_item_product_code,linked_account_id,month,year
order by year,month desc''')




cost_insights_df=cost_insights_df.withColumn('concated_tags',concat_ws("||",*filtered_list))

cost_insights_df=cost_insights_df.withColumn('tag_dim_id',generate_sha(cost_insights_df.concated_tags))


cost_insights_df=cost_insights_df.drop("concated_tags")
cost_insights_df=cost_insights_df.drop(*filtered_list)



cost_insights_df=cost_insights_df.withColumn('concated_tags_resource',concat_ws("||",*resource_dim_column_list))

cost_insights_df=cost_insights_df.withColumn('aws_product_dim_id',generate_sha(cost_insights_df.concated_tags_resource))


cost_insights_df=cost_insights_df.drop("concated_tags_resource")

#for AWS DT resolution
resource_dim_column_list=['line_item_operation', 'line_item_product_code', 'product_product_family', 'product_region', 'product_servicecode']
cost_insights_df=cost_insights_df.drop(*resource_dim_column_list)

##########AWS DT CALC ##############

dt_df.repartition("date_dim_id").write.mode("overwrite").partitionBy("year","month").parquet(args['dt_s3_output_path'])

##########AWS DT CALC ##############







cost_insights_df.repartition("date_dim_id").write.mode("overwrite").partitionBy("year","month").parquet(args['s3_output_path'])
job.commit()











