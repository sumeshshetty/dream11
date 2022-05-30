import awswrangler as wr
from awsglue.utils import getResolvedOptions
import datetime
import pytz
import sys
import boto3
import time
import pandas as pd
client = boto3.client('athena')

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.datetime.now(tz)


args = getResolvedOptions(sys.argv,['JOB_NAME','athena_output_location'])




args['raw_table']='raw_cost_usage_hourly_athena'
args['raw_table_second']='raw_cur_data_sporta_management'

args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'

#args['athena_output_location']-'s3://acc-buildapp-test/athena-results/'


wr_df=wr.catalog.table(database=args['raw_database'], table=args['raw_table'])


wr_df_second=wr.catalog.table(database=args['raw_database'], table=args['raw_table_second'])





########## for first raw table #################################

colnm=wr_df[wr_df['Column Name'].str.contains("resource_tags_user_")]
colnm_list=colnm['Column Name'].values.tolist()
colnm_list.sort()

colnm_list_1=[ "Coalesce(nullif("+s+",''),'') as "+s for s in colnm_list ]


colnm_str=','.join(colnm_list)
colnm_str_1=','.join(colnm_list_1)


########## for first raw table ################################# 


########## for second raw table #################################

colnm_sec=wr_df_second[wr_df_second['Column Name'].str.contains("resource_tags_user_")]
colnm_list_sec=colnm_sec['Column Name'].values.tolist()
colnm_list_sec.sort()

colnm_list_1_sec=[ "Coalesce(nullif("+s+",''),'') as "+s for s in colnm_list_sec ]
colnm_str_sec=','.join(colnm_list_sec)
colnm_str_1_sec=','.join(colnm_list_1_sec)



########## for second raw table ################################# 

resource_dim_column_list=['line_item_operation', 'line_item_product_code', 'product_product_family', 'product_region', 'product_servicecode']
resource_dim_column_list.sort()
colnm_str_resource=','.join(resource_dim_column_list)


query_str=''' 

CREATE OR REPLACE VIEW {}.summary_view AS 
SELECT
  "year"
, "month"
, "bill_billing_period_start_date" "billing_period"
, "date_trunc"('day', "line_item_usage_start_date") "usage_date"
, "bill_payer_account_id" "payer_account_id"
, "line_item_usage_account_id" "linked_account_id"
, "bill_invoice_id" "invoice_id"
, "product_sku"
, "line_item_line_item_type" "charge_type"

, (CASE WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN 'Running_Usage' WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN 'Running_Usage' WHEN ("line_item_line_item_type" = 'Usage') THEN 'Running_Usage' ELSE 'non_usage' END) "charge_category"
, (CASE WHEN ("savings_plan_savings_plan_a_r_n" <> '') THEN 'SavingsPlan' WHEN ("reservation_reservation_a_r_n" <> '') THEN 'Reserved' WHEN ("line_item_usage_type" LIKE '%Spot%') THEN 'Spot' ELSE 'OnDemand' END) "purchase_option"
, (CASE WHEN ("savings_plan_savings_plan_a_r_n" <> '') THEN "savings_plan_savings_plan_a_r_n" WHEN ("reservation_reservation_a_r_n" <> '') THEN "reservation_reservation_a_r_n" ELSE '' END) "ri_sp_arn"
, "line_item_product_code" "product_code"
, "product_product_name" "product_name"
, (CASE WHEN (("bill_billing_entity" = 'AWS Marketplace') AND (NOT ("line_item_line_item_type" LIKE '%Discount%'))) THEN "Product_Product_Name" WHEN ("product_servicecode" = '') THEN "line_item_product_code" ELSE "product_servicecode" END) "service"
, "product_product_family" "product_family"
, "line_item_usage_type" "usage_type"
, "line_item_operation" "operation"
, "line_item_line_item_description" "item_description"
, "line_item_availability_zone" "availability_zone"
, "product_region" "region"
, (CASE WHEN ((("line_item_usage_type" LIKE '%Spot%') AND ("line_item_product_code" = 'AmazonEC2')) AND ("line_item_line_item_type" = 'Usage')) THEN "split_part"("line_item_line_item_description", '.', 1) ELSE "product_instance_type_family" END) "instance_type_family"
, (CASE WHEN ((("line_item_usage_type" LIKE '%Spot%') AND ("line_item_product_code" = 'AmazonEC2')) AND ("line_item_line_item_type" = 'Usage')) THEN "split_part"("line_item_line_item_description", ' ', 1) ELSE "product_instance_type" END) "instance_type"
, (CASE WHEN ((("line_item_usage_type" LIKE '%Spot%') AND ("line_item_product_code" = 'AmazonEC2')) AND ("line_item_line_item_type" = 'Usage')) THEN "split_part"("split_part"("line_item_line_item_description", ' ', 2), '/', 1) ELSE "product_operating_system" END) "platform"
, "product_tenancy" "tenancy"
, "product_physical_processor" "processor"
, "product_processor_features" "processor_features"
, "product_database_engine" "database_engine"
, "product_group" "product_group"
, "product_from_location" "product_from_location"
, "product_to_location" "product_to_location"
, "product_current_generation" "current_generation"
, "line_item_legal_entity" "legal_entity"
, "bill_billing_entity" "billing_entity"
, "pricing_unit" "pricing_unit"
, "approx_distinct"("line_item_resource_id") "resource_id_count"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN "line_item_usage_amount" WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN "line_item_usage_amount" WHEN ("line_item_line_item_type" = 'Usage') THEN "line_item_usage_amount" ELSE 0 END)) "usage_quantity"
, "sum"("line_item_unblended_cost") "unblended_cost"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN "savings_plan_savings_plan_effective_cost" WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN ("savings_plan_total_commitment_to_date" - "savings_plan_used_commitment") WHEN ("line_item_line_item_type" = 'SavingsPlanNegation') THEN 0 WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN 0 WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN "reservation_effective_cost" WHEN ("line_item_line_item_type" = 'RIFee') THEN ("reservation_unused_amortized_upfront_fee_for_billing_period" + "reservation_unused_recurring_fee") WHEN (("line_item_line_item_type" = 'Fee') AND ("reservation_reservation_a_r_n" <> '')) THEN 0 ELSE "line_item_unblended_cost" END)) "amortized_cost"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN -"savings_plan_amortized_upfront_commitment_for_billing_period" WHEN ("line_item_line_item_type" = 'RIFee') THEN -"reservation_amortized_upfront_fee_for_billing_period" ELSE 0 END)) "ri_sp_trueup"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN "line_item_unblended_cost" WHEN (("line_item_line_item_type" = 'Fee') AND ("reservation_reservation_a_r_n" <> '')) THEN "line_item_unblended_cost" ELSE 0 END)) "ri_sp_upfront_fees"
, "sum"((CASE WHEN ("line_item_line_item_type" <> 'SavingsPlanNegation') THEN "pricing_public_on_demand_cost" ELSE 0 END)) "public_cost"
,
(CASE 
    


WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonApiGateway' )) THEN 'Amazon API Gateway'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonAthena' )) THEN 'Amazon Athena'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonCloudFront' )) THEN 'Amazon CloudFront'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonCloudWatch' )) THEN 'AmazonCloudWatch'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonCognito' )) THEN 'Amazon Cognito'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonDocDB' )) THEN 'Amazon DocumentDB (with MongoDB compatibility)'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonDynamoDB' )) THEN 'Amazon DynamoDB'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonEC2' )) THEN 'Amazon Elastic Compute Cloud'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonECR' )) THEN 'Amazon EC2 Container Registry (ECR)'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonECRPublic' )) THEN 'Amazon Elastic Container Registry Public'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonECS' )) THEN 'Amazon Elastic Container Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonEFS' )) THEN 'Amazon Elastic File System'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonEKS' )) THEN 'Amazon Elastic Container Service for Kubernetes'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonElastiCache' )) THEN 'Amazon ElastiCache'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonES' )) THEN 'Amazon Elasticsearch Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonForecast' )) THEN 'Amazon Forecast'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonGlacier' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonGuardDuty' )) THEN 'Amazon GuardDuty'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonInspector' )) THEN 'Amazon Inspector'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonKinesis' )) THEN 'Amazon Kinesis'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonKinesisAnalytics' )) THEN 'Amazon Kinesis Analytics'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonKinesisFirehose' )) THEN 'Amazon Kinesis Firehose'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonLightsail' )) THEN 'Amazon Lightsail'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMCS' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMQ' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMSK' )) THEN 'Amazon Managed Streaming for Apache Kafka'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMWAA' )) THEN 'Amazon Managed Workflows for Apache Airflow'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonNeptune' )) THEN 'Amazon Neptune'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonPersonalize' )) THEN 'Amazon Personalize'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonPolly' )) THEN 'Amazon Polly'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonQuickSight' )) THEN 'Amazon QuickSight'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRDS' )) THEN 'Amazon Relational Database Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRedshift' )) THEN 'Amazon Redshift'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRekognition' )) THEN 'Amazon Rekognition'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRoute53' )) THEN 'Amazon Route 53'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonS3' )) THEN 'Amazon Simple Storage Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonS3GlacierDeepArchive' )) THEN 'Amazon S3 Glacier Deep Archive'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSageMaker' )) THEN 'Amazon SageMaker'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSES' )) THEN 'Amazon Simple Email Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSimpleDB' )) THEN 'Amazon SimpleDB'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSNS' )) THEN 'Amazon Simple Notification Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonStates' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonTimestream' )) THEN 'Amazon Timestream'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonVPC' )) THEN 'Amazon Virtual Private Cloud'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonWorkDocs' )) THEN 'Amazon WorkDocs'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonWorkSpaces' )) THEN 'Amazon WorkSpaces'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AppFlow' )) THEN 'Amazon AppFlow'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSAmplify' )) THEN 'AWS Amplify'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSAppSync' )) THEN 'AWS AppSync'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSBackup' )) THEN 'AWS Backup'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSBudgets' )) THEN 'AWS Budgets'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCertificateManager' )) THEN 'AWS Certificate Manager'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCloudShell' )) THEN 'AWS CloudShell'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCloudTrail' )) THEN 'AWS CloudTrail'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCodeArtifact' )) THEN 'AWS CodeArtifact'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCodeCommit' )) THEN 'AWS CodeCommit'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCodePipeline' )) THEN 'AWS CodePipeline'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSConfig' )) THEN 'AWS Config'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCostExplorer' )) THEN 'AWS Cost Explorer'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSDatabaseMigrationSvc' )) THEN 'AWS Database Migration Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSDataTransfer' )) THEN 'AWS Data Transfer'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSDirectoryService' )) THEN 'AWS Directory Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSELB' )) THEN 'Elastic Load Balancing'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaConvert' )) THEN 'AWS Elemental MediaConvert'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaLive' )) THEN 'AWS Elemental MediaLive'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaPackage' )) THEN 'AWS Elemental MediaPackage'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaStore' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaTailor' )) THEN 'AWS Elemental MediaTailor'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSEvents' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSGlobalAccelerator' )) THEN 'AWS Global Accelerator'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSGlue' )) THEN 'AWS Glue'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='awskms' )) THEN 'AWS Key Management Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSLambda' )) THEN 'AWS Lambda'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSQueueService' )) THEN 'Amazon Simple Queue Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSSecretsManager' )) THEN 'AWS Secrets Manager'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSSecurityHub' )) THEN 'AWS Security Hub'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSShield' )) THEN 'AWS Shield'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSSystemsManager' )) THEN 'AWS Systems Manager'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSTransfer' )) THEN 'AWS Transfer Family'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='awswaf' )) THEN 'AWS WAF'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSXRay' )) THEN 'AWS X-Ray'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='CodeBuild' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='comprehend' )) THEN 'Amazon Comprehend'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='ComputeSavingsPlans' )) THEN 'Savings Plans for AWS Compute usage'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='datapipeline' )) THEN 'AWS Data Pipeline'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='ElasticMapReduce' )) THEN 'Amazon Elastic MapReduce'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='OCBElasticComputeCloud' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='OCBPremiumSupport' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='translate' )) THEN 'Amazon Translate'
    
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonEC2' )) THEN 'Amazon Elastic Compute Cloud'
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonRedshift' )) THEN 'Amazon Redshift'
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonElastiCache' )) THEN 'Amazon ElastiCache'
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonRDS' )) THEN 'Amazon Relational Database Service'

WHEN (("product_servicename" = '') AND ( "line_item_product_code"='OCBElasticComputeCloud' ) AND ("line_item_line_item_type"='Fee')) THEN 'OCBElasticComputeCloud'

WHEN (("product_servicename" = '') AND ("bill_billing_entity" = 'AWS Marketplace')) THEN 'AWS Marketplace ' || "product_product_name" 

ELSE "product_servicename"



END) 
"product_servicename"

, {}
,{}
FROM
  {}
WHERE (("bill_billing_period_start_date" >= ("date_trunc"('month', current_timestamp) - INTERVAL  '7' MONTH)) AND (CAST("concat"("year", '-', "month", '-01') AS date) >= ("date_trunc"('month', current_date) - INTERVAL  '7' MONTH)))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,product_servicename,{},{}


 '''.format(args['raw_database'],colnm_str_1,colnm_str_resource,args['raw_database']+"."+args['raw_table'],colnm_str,colnm_str_resource)


################################ FOR SECOND TABLE ############

query_str_sec=''' 

CREATE OR REPLACE VIEW {}.summary_view_second AS 
SELECT
  "year"
, "month"
, "bill_billing_period_start_date" "billing_period"
, "date_trunc"('day', "line_item_usage_start_date") "usage_date"
, "bill_payer_account_id" "payer_account_id"
, "line_item_usage_account_id" "linked_account_id"
, "bill_invoice_id" "invoice_id"
, "product_sku"
, "line_item_line_item_type" "charge_type"

, (CASE WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN 'Running_Usage' WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN 'Running_Usage' WHEN ("line_item_line_item_type" = 'Usage') THEN 'Running_Usage' ELSE 'non_usage' END) "charge_category"
, (CASE WHEN ("savings_plan_savings_plan_a_r_n" <> '') THEN 'SavingsPlan'  WHEN ("line_item_usage_type" LIKE '%Spot%') THEN 'Spot' ELSE 'OnDemand' END) "purchase_option"
, (CASE WHEN ("savings_plan_savings_plan_a_r_n" <> '') THEN "savings_plan_savings_plan_a_r_n"  ELSE '' END) "ri_sp_arn"
, "line_item_product_code" "product_code"
, "product_product_name" "product_name"
, (CASE WHEN (("bill_billing_entity" = 'AWS Marketplace') AND (NOT ("line_item_line_item_type" LIKE '%Discount%'))) THEN "Product_Product_Name" WHEN ("product_servicecode" = '') THEN "line_item_product_code" ELSE "product_servicecode" END) "service"
, "product_product_family" "product_family"
, "line_item_usage_type" "usage_type"
, "line_item_operation" "operation"
, "line_item_line_item_description" "item_description"
, "line_item_availability_zone" "availability_zone"
, "product_region" "region"
, (CASE WHEN ((("line_item_usage_type" LIKE '%Spot%') AND ("line_item_product_code" = 'AmazonEC2')) AND ("line_item_line_item_type" = 'Usage')) THEN "split_part"("line_item_line_item_description", '.', 1) ELSE "product_instance_type_family" END) "instance_type_family"
, (CASE WHEN ((("line_item_usage_type" LIKE '%Spot%') AND ("line_item_product_code" = 'AmazonEC2')) AND ("line_item_line_item_type" = 'Usage')) THEN "split_part"("line_item_line_item_description", ' ', 1) ELSE "product_instance_type" END) "instance_type"
, (CASE WHEN ((("line_item_usage_type" LIKE '%Spot%') AND ("line_item_product_code" = 'AmazonEC2')) AND ("line_item_line_item_type" = 'Usage')) THEN "split_part"("split_part"("line_item_line_item_description", ' ', 2), '/', 1) ELSE "product_operating_system" END) "platform"
, "product_tenancy" "tenancy"
, "product_physical_processor" "processor"
, "product_processor_features" "processor_features"
, "product_database_engine" "database_engine"
, "product_group" "product_group"
, "product_from_location" "product_from_location"
, "product_to_location" "product_to_location"
, "product_current_generation" "current_generation"
, "line_item_legal_entity" "legal_entity"
, "bill_billing_entity" "billing_entity"
, "pricing_unit" "pricing_unit"
, "approx_distinct"("line_item_resource_id") "resource_id_count"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN "line_item_usage_amount" WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN "line_item_usage_amount" WHEN ("line_item_line_item_type" = 'Usage') THEN "line_item_usage_amount" ELSE 0 END)) "usage_quantity"
, "sum"("line_item_unblended_cost") "unblended_cost"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanCoveredUsage') THEN "savings_plan_savings_plan_effective_cost" WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN ("savings_plan_total_commitment_to_date" - "savings_plan_used_commitment") WHEN ("line_item_line_item_type" = 'SavingsPlanNegation') THEN 0 WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN 0 WHEN ("line_item_line_item_type" = 'DiscountedUsage') THEN "reservation_effective_cost" WHEN ("line_item_line_item_type" = 'RIFee') THEN ("reservation_unused_amortized_upfront_fee_for_billing_period" + "reservation_unused_recurring_fee") WHEN (("line_item_line_item_type" = 'Fee') ) THEN 0 ELSE "line_item_unblended_cost" END)) "amortized_cost"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanRecurringFee') THEN -"savings_plan_amortized_upfront_commitment_for_billing_period" WHEN ("line_item_line_item_type" = 'RIFee') THEN -"reservation_amortized_upfront_fee_for_billing_period" ELSE 0 END)) "ri_sp_trueup"
, "sum"((CASE WHEN ("line_item_line_item_type" = 'SavingsPlanUpfrontFee') THEN "line_item_unblended_cost" WHEN (("line_item_line_item_type" = 'Fee') ) THEN "line_item_unblended_cost" ELSE 0 END)) "ri_sp_upfront_fees"
, "sum"((CASE WHEN ("line_item_line_item_type" <> 'SavingsPlanNegation') THEN "pricing_public_on_demand_cost" ELSE 0 END)) "public_cost"
,(CASE 
    


WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonApiGateway' )) THEN 'Amazon API Gateway'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonAthena' )) THEN 'Amazon Athena'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonCloudFront' )) THEN 'Amazon CloudFront'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonCloudWatch' )) THEN 'AmazonCloudWatch'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonCognito' )) THEN 'Amazon Cognito'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonDocDB' )) THEN 'Amazon DocumentDB (with MongoDB compatibility)'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonDynamoDB' )) THEN 'Amazon DynamoDB'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonEC2' )) THEN 'Amazon Elastic Compute Cloud'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonECR' )) THEN 'Amazon EC2 Container Registry (ECR)'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonECRPublic' )) THEN 'Amazon Elastic Container Registry Public'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonECS' )) THEN 'Amazon Elastic Container Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonEFS' )) THEN 'Amazon Elastic File System'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonEKS' )) THEN 'Amazon Elastic Container Service for Kubernetes'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonElastiCache' )) THEN 'Amazon ElastiCache'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonES' )) THEN 'Amazon Elasticsearch Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonForecast' )) THEN 'Amazon Forecast'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonGlacier' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonGuardDuty' )) THEN 'Amazon GuardDuty'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonInspector' )) THEN 'Amazon Inspector'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonKinesis' )) THEN 'Amazon Kinesis'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonKinesisAnalytics' )) THEN 'Amazon Kinesis Analytics'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonKinesisFirehose' )) THEN 'Amazon Kinesis Firehose'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonLightsail' )) THEN 'Amazon Lightsail'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMCS' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMQ' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMSK' )) THEN 'Amazon Managed Streaming for Apache Kafka'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonMWAA' )) THEN 'Amazon Managed Workflows for Apache Airflow'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonNeptune' )) THEN 'Amazon Neptune'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonPersonalize' )) THEN 'Amazon Personalize'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonPolly' )) THEN 'Amazon Polly'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonQuickSight' )) THEN 'Amazon QuickSight'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRDS' )) THEN 'Amazon Relational Database Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRedshift' )) THEN 'Amazon Redshift'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRekognition' )) THEN 'Amazon Rekognition'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonRoute53' )) THEN 'Amazon Route 53'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonS3' )) THEN 'Amazon Simple Storage Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonS3GlacierDeepArchive' )) THEN 'Amazon S3 Glacier Deep Archive'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSageMaker' )) THEN 'Amazon SageMaker'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSES' )) THEN 'Amazon Simple Email Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSimpleDB' )) THEN 'Amazon SimpleDB'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonSNS' )) THEN 'Amazon Simple Notification Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonStates' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonTimestream' )) THEN 'Amazon Timestream'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonVPC' )) THEN 'Amazon Virtual Private Cloud'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonWorkDocs' )) THEN 'Amazon WorkDocs'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AmazonWorkSpaces' )) THEN 'Amazon WorkSpaces'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AppFlow' )) THEN 'Amazon AppFlow'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSAmplify' )) THEN 'AWS Amplify'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSAppSync' )) THEN 'AWS AppSync'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSBackup' )) THEN 'AWS Backup'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSBudgets' )) THEN 'AWS Budgets'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCertificateManager' )) THEN 'AWS Certificate Manager'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCloudShell' )) THEN 'AWS CloudShell'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCloudTrail' )) THEN 'AWS CloudTrail'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCodeArtifact' )) THEN 'AWS CodeArtifact'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCodeCommit' )) THEN 'AWS CodeCommit'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCodePipeline' )) THEN 'AWS CodePipeline'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSConfig' )) THEN 'AWS Config'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSCostExplorer' )) THEN 'AWS Cost Explorer'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSDatabaseMigrationSvc' )) THEN 'AWS Database Migration Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSDataTransfer' )) THEN 'AWS Data Transfer'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSDirectoryService' )) THEN 'AWS Directory Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSELB' )) THEN 'Elastic Load Balancing'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaConvert' )) THEN 'AWS Elemental MediaConvert'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaLive' )) THEN 'AWS Elemental MediaLive'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaPackage' )) THEN 'AWS Elemental MediaPackage'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaStore' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSElementalMediaTailor' )) THEN 'AWS Elemental MediaTailor'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSEvents' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSGlobalAccelerator' )) THEN 'AWS Global Accelerator'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSGlue' )) THEN 'AWS Glue'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='awskms' )) THEN 'AWS Key Management Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSLambda' )) THEN 'AWS Lambda'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSQueueService' )) THEN 'Amazon Simple Queue Service'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSSecretsManager' )) THEN 'AWS Secrets Manager'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSSecurityHub' )) THEN 'AWS Security Hub'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSShield' )) THEN 'AWS Shield'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSSystemsManager' )) THEN 'AWS Systems Manager'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSTransfer' )) THEN 'AWS Transfer Family'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='awswaf' )) THEN 'AWS WAF'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='AWSXRay' )) THEN 'AWS X-Ray'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='CodeBuild' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='comprehend' )) THEN 'Amazon Comprehend'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='ComputeSavingsPlans' )) THEN 'Savings Plans for AWS Compute usage'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='datapipeline' )) THEN 'AWS Data Pipeline'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='ElasticMapReduce' )) THEN 'Amazon Elastic MapReduce'
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='OCBElasticComputeCloud' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='OCBPremiumSupport' )) THEN ''
WHEN (("product_servicename" = 'AWS Data Transfer') AND ( "line_item_product_code"='translate' )) THEN 'Amazon Translate'

WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonEC2' )) THEN 'Amazon Elastic Compute Cloud'
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonRedshift' )) THEN 'Amazon Redshift'
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonElastiCache' )) THEN 'Amazon ElastiCache'
WHEN (("product_servicename" = '') AND ( "line_item_product_code"='AmazonRDS' )) THEN 'Amazon Relational Database Service'

WHEN (("product_servicename" = '') AND ( "line_item_product_code"='OCBElasticComputeCloud' ) AND ("line_item_line_item_type"='Fee')) THEN 'OCBElasticComputeCloud'

WHEN (("product_servicename" = '') AND ("bill_billing_entity" = 'AWS Marketplace')) THEN 'AWS Marketplace ' || "product_product_name" 


ELSE "product_servicename"
END) 
"product_servicename"
, {}
,{}
FROM
  {}
WHERE (("bill_billing_period_start_date" >= ("date_trunc"('month', current_timestamp) - INTERVAL  '7' MONTH)) AND (CAST("concat"("year", '-', "month", '-01') AS date) >= ("date_trunc"('month', current_date) - INTERVAL  '7' MONTH)))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,product_servicename,{},{}


 '''.format(args['raw_database'],colnm_str_1_sec,colnm_str_resource,args['raw_database']+"."+args['raw_table_second'],colnm_str_sec,colnm_str_resource)

################################ FOR SECOND TABLE ############ 

response = client.start_query_execution( QueryString=query_str,
ResultConfiguration={
        'OutputLocation': args['athena_output_location']
        
    })
execution_id = response['QueryExecutionId']
print(response)

################################ FOR SECOND TABLE ############ 
response_sec = client.start_query_execution( QueryString=query_str_sec,
ResultConfiguration={
        'OutputLocation': args['athena_output_location']
        
    })
execution_id_sec = response_sec['QueryExecutionId']
print(response_sec)
################################ FOR SECOND TABLE ############ 



status = ''
status_sec = ''

while True:
    stats = client.get_query_execution(QueryExecutionId=execution_id)
    status = stats['QueryExecution']['Status']['State']
    if status in ['SUCCEEDED','CANCELLED','FAILED']:
        print(f"query sucessfull ...breaking.. {status}")
        break
    time.sleep(0.2)  # 200ms



while True:
    stats_sec = client.get_query_execution(QueryExecutionId=execution_id_sec)
    status_sec = stats_sec['QueryExecution']['Status']['State']
    if status_sec in ['SUCCEEDED','CANCELLED','FAILED']:
        print(f"query sucessfull ...breaking.. {status_sec}")
        break
    time.sleep(0.2)  # 200ms




