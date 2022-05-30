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
print("len of first",len(wr_df))

wr_df_second=wr.catalog.table(database=args['raw_database'], table=args['raw_table_second'])
print("len of second",len(wr_df_second))

#wr_df=wr_df.union(wr_df_second)
# concat_list=[wr_df,wr_df_second]
wr_df=wr_df.append(wr_df_second)
wr_df.drop_duplicates(keep='first',inplace=True)
print("len of union",len(wr_df))




colnm=wr_df[wr_df['Column Name'].str.contains("resource_tags_user_")]
colnm_list=colnm['Column Name'].values.tolist()
colnm_list.sort()
#to remove duplicates
colnm_list = list(set(colnm_list))
colnm_list_1=[ "Coalesce(nullif("+s+",''),'') as "+s for s in colnm_list ]


colnm_str=','.join(colnm_list)
colnm_str_1=','.join(colnm_list_1)


#product_servicename is part of resource_dim_column_list but is moved to replace view beacsue of conditions
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
(CASE WHEN (("product_servicename" = 'AWS Data Transfer') AND ( ("line_item_product_code" = 'AmazonS3'))) THEN "AWS Simple Storage service" 
    WHEN ("product_servicecode" = '') THEN "line_item_product_code" ELSE "product_servicecode" END) 
"product_servicename"

, {}
,{}
FROM
  {}
WHERE (("bill_billing_period_start_date" >= ("date_trunc"('month', current_timestamp) - INTERVAL  '7' MONTH)) AND (CAST("concat"("year", '-', "month", '-01') AS date) >= ("date_trunc"('month', current_date) - INTERVAL  '7' MONTH)))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,36,{},{}


 '''.format(args['raw_database'],colnm_str_1,colnm_str_resource,args['raw_database']+"."+args['raw_table'],colnm_str,colnm_str_resource)


response = client.start_query_execution( QueryString=query_str,
ResultConfiguration={
        'OutputLocation': args['athena_output_location']
        
    })
execution_id = response['QueryExecutionId']
print(response)
status = ''
while True:
    stats = client.get_query_execution(QueryExecutionId=execution_id)
    status = stats['QueryExecution']['Status']['State']
    if status in ['SUCCEEDED','CANCELLED','FAILED']:
        print(f"query sucessfull ...breaking.. {status}")
        break
    time.sleep(0.2)  # 200ms




