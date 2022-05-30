import boto3
import botocore
import pandas as pd
import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime,timedelta
import pytz
from pyspark.sql.functions import round, col
from dateutil.tz import tzlocal

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)


request_arn='arn:aws:iam::078210713173:role/proserve-to-production-ri-sp-assume-role'
db_table_utilization="acc_cost_v2.ri_utilization"
stag_db_table_utilization='acc_cost_v2.stage_ri_utilization'

db_table_coverage='acc_cost_v2.ri_coverage'
stag_db_table_coverage='acc_cost_v2.stage_ri_coverage'
db_name='prodinsightsdb'


#for mandatory parameters
#previous_data for initial run upto 1 year
args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir','previous_data'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
logger = glueContext.get_logger()
#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) and ('--{}'.format('day') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year','day'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
    args['day'] = args_opt['day']
else:
    logger.info("setting optional parameters")
    
    args['year']=str(current_date.year)
    args['month']=str(current_date.month).lstrip("0")
    args['day']=str(current_date.day)

job.init(args['JOB_NAME'], args)

assume_role_cache: dict = {}
def assumed_role_session(role_arn: str, base_session: botocore.session.Session = None):
    base_session = base_session or boto3.session.Session()._session
    fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
        client_creator = base_session.create_client,
        source_credentials = base_session.get_credentials(),
        role_arn = role_arn,
        extra_args = {}
    )
    creds = botocore.credentials.DeferredRefreshableCredentials(
        method = 'assume-role',
        refresh_using = fetcher.fetch_credentials,
        time_fetcher = lambda: datetime.now(tzlocal())
    )
    botocore_session = botocore.session.Session()
    botocore_session._credentials = creds
    return boto3.Session(botocore_session = botocore_session)


def create_date_dim_id(date_str):
    return str(date_str).replace("-","")

if len(args['month'])==1:
        args['month']="0"+args['month']
    
    
if len(args['day'])==1:
    args['day']="0"+args['day']


if args['previous_data']=='true':

    end_date=args['year']+"-"+args['month']+"-"+args['day']
    prev_year=int(args['year'])-1
    start_date=str(prev_year)+"-"+args['month']+"-"+args['day']
else:
    end_date=args['year']+"-"+args['month']+"-"+args['day']
    start_date=str(datetime.strptime(end_date,"%Y-%m-%d")-timedelta(days=1)).split(" ")[0]
    start_date_list=start_date.split("-")
    

print(f"calcuting for ... {start_date} to {end_date}")


session = assumed_role_session(request_arn)
client_ce=session.client('ce')

utilization_response = client_ce.get_reservation_utilization(
    TimePeriod={
        'Start': start_date,
        'End': end_date
    })
    
coverage_response = client_ce.get_reservation_coverage(
    TimePeriod={
        'Start': start_date,
        'End': end_date
    },
    
    Granularity='DAILY')

utilization_data=utilization_response['UtilizationsByTime']
coverage_data=coverage_response['CoveragesByTime']



utilization_df=pd.json_normalize(utilization_data)
coverage_df=pd.json_normalize(coverage_data)


utilization_df['date_dim_id'] = utilization_df.apply(lambda row : create_date_dim_id(row['TimePeriod.Start']), axis = 1)
utilization_df=utilization_df.drop(columns=['TimePeriod.Start','TimePeriod.End'])
utilization_df.columns = utilization_df.columns.str.replace("Total.", "")



coverage_df['date_dim_id'] = coverage_df.apply(lambda row : create_date_dim_id(row['TimePeriod.Start']), axis = 1)
coverage_df=coverage_df.drop(columns=['TimePeriod.Start','TimePeriod.End'])
coverage_df.columns = coverage_df.columns.str.replace("Total.", "")
#as we have nested json
coverage_df.columns = coverage_df.columns.str.replace(".", "_")

print("########## COVERAGE ##########")
print(coverage_df.head(5))
print("########## COVERAGE ##########")

print("########## UTILIZATION ##########")
print(utilization_df.head(5))
print("########## UTILIZATION ##########")

utilization_sparkDF=spark.createDataFrame(utilization_df) 
coverage_sparkDF=spark.createDataFrame(coverage_df) 


utilization_dyf = DynamicFrame.fromDF(utilization_sparkDF, glueContext, "utilization_dyf")
coverage_dyf = DynamicFrame.fromDF(coverage_sparkDF, glueContext, "coverage_dyf")




pre_query_utilization = "begin; "\
        "drop table if exists acc_cost_v2.stage_ri_utilization; "\
        "create table acc_cost_v2.stage_ri_utilization as "\
        "select * from acc_cost_v2.ri_utilization "\
        "where 1=2; "\
        "end;"

pre_query_coverage = "begin; "\
        "drop table if exists acc_cost_v2.stage_ri_coverage; "\
        "create table acc_cost_v2.stage_ri_coverage as "\
        "select * from acc_cost_v2.ri_coverage "\
        "where 1=2; "\
        "end;"
    
post_query_utilization = "begin; delete from acc_cost_v2.ri_utilization using acc_cost_v2.stage_ri_utilization where acc_cost_v2.stage_ri_utilization.date_dim_id =acc_cost_v2.ri_utilization.date_dim_id ; end;"
post_query_coverage = "begin; delete from acc_cost_v2.ri_coverage using acc_cost_v2.stage_ri_coverage where acc_cost_v2.stage_ri_coverage.date_dim_id =acc_cost_v2.ri_coverage.date_dim_id ; end;"
  
#utilization_datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = utilization_dyf, catalog_connection = "acc_redshift", connection_options = {"dbtable": db_table_utilization, "database":db_name,"preactions":pre_query_utilization}, redshift_tmp_dir = args["TempDir"]+"/ri_utilization", transformation_ctx = "utilization_datasink4")
#coverage_datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = coverage_dyf, catalog_connection = "acc_redshift", connection_options = {"dbtable": db_table_coverage, "database":db_name,"preactions":pre_query_coverage}, redshift_tmp_dir = args["TempDir"]+"/ri_coverage", transformation_ctx = "coverage_datasink4")

job.commit()
logger.info("***succesfull execution**")


    