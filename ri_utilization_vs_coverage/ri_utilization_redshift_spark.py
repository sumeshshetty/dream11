
#when previous_data is false and no month ,day,year is specified then script will pick current day -2 day for processinf
#i.e if current day is 2021-11-26 then start day will be 2021-11-24 and end day will be 2021-11-25
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
stage_db_table_utilization='acc_cost_v2.stage_ri_utilization'

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
    previous_date=current_date-timedelta(days=1)
    args['year']=str(previous_date.year)
    args['month']=str(previous_date.month).lstrip("0")
    args['day']=str(previous_date.day)

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
    

print(f"calcuting for ... {start_date} to {end_date}")


session = assumed_role_session(request_arn)
client_ce=session.client('ce')

try:
    ec2_utilization_response = client_ce.get_reservation_utilization(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Filter={'Dimensions': {
                'Key': 'SERVICE',
                'Values': [
                    'Amazon Elastic Compute Cloud - Compute'
                ]
            }},
        Granularity='DAILY')
except Exception as e:
    print(e)
    sys.exit(e)

rds_utilization_response = client_ce.get_reservation_utilization(
    TimePeriod={
        'Start': start_date,
        'End': end_date
    },
    Filter={'Dimensions': {
            'Key': 'SERVICE',
            'Values': [
                'Amazon Relational Database Service'
            ]
        }},
    Granularity='DAILY')

redshift_utilization_response = client_ce.get_reservation_utilization(
    TimePeriod={
        'Start': start_date,
        'End': end_date
    },
    Filter={'Dimensions': {
            'Key': 'SERVICE',
            'Values': [
                'Amazon Redshift'
            ]
        }},
    Granularity='DAILY')




elasticache_utilization_response = client_ce.get_reservation_utilization(
    TimePeriod={
        'Start': start_date,
        'End': end_date
    },
    Filter={'Dimensions': {
            'Key': 'SERVICE',
            'Values': [
                'Amazon ElastiCache'
            ]
        }},
    Granularity='DAILY')
    


ec2_utilization_data=ec2_utilization_response['UtilizationsByTime']
for item in ec2_utilization_data:
    item['service_name']='AMAZON EC2'

rds_utilization_data=rds_utilization_response['UtilizationsByTime']
for item in rds_utilization_data:
    item['service_name']='AMAZON RDS'

    
redshift_utilization_data=redshift_utilization_response['UtilizationsByTime']
for item in redshift_utilization_data:
    item['service_name']='AMAZON REDSHIFT'

elasticache_utilization_data=elasticache_utilization_response['UtilizationsByTime']
for item in elasticache_utilization_data:
    item['service_name']='AMAZON ELASTICACHE'


ec2_utilization_df=pd.json_normalize(ec2_utilization_data)
rds_utilization_df=pd.json_normalize(rds_utilization_data)
redshift_utilization_df=pd.json_normalize(redshift_utilization_data)
elasticache_utilization_df=pd.json_normalize(elasticache_utilization_data)

service_list=[ec2_utilization_df,rds_utilization_df,redshift_utilization_df,elasticache_utilization_df]
utilization_df = pd.concat(service_list)





utilization_df['date_dim_id'] = utilization_df.apply(lambda row : create_date_dim_id(row['TimePeriod.Start']), axis = 1)
utilization_df=utilization_df.drop(columns=['TimePeriod.Start','TimePeriod.End'])




utilization_df.columns = utilization_df.columns.str.replace(".", "_")
utilization_df.columns = utilization_df.columns.str.replace("Total_", "")


all_columns = list(utilization_df) # Creates list of all column headers
utilization_df[all_columns] = utilization_df[all_columns].astype(str)
utilization_sparkDF=spark.createDataFrame(utilization_df) 



utilization_dyf = DynamicFrame.fromDF(utilization_sparkDF, glueContext, "utilization_dyf")




pre_query_utilization = "begin; "\
        "drop table if exists acc_cost_v2.stage_ri_utilization; "\
        "create table acc_cost_v2.stage_ri_utilization as "\
        "select * from acc_cost_v2.ri_utilization "\
        "where 1=2; "\
        "end;"


    
post_query_utilization = "begin; delete from acc_cost_v2.ri_utilization using acc_cost_v2.stage_ri_utilization where acc_cost_v2.stage_ri_utilization.date_dim_id =acc_cost_v2.ri_utilization.date_dim_id; insert into acc_cost_v2.ri_utilization select * from acc_cost_v2.stage_ri_utilization; end;"

utilization_datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = utilization_dyf, catalog_connection = "acc_redshift", connection_options = {"dbtable": stage_db_table_utilization, "database":db_name,"preactions":pre_query_utilization,"postactions":post_query_utilization}, redshift_tmp_dir = args["TempDir"]+"/ri_utilization", transformation_ctx = "utilization_datasink4")

job.commit()
logger.info("***succesfull execution**")



    