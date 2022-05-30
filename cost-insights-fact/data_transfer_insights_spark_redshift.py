import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import pytz

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)

#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir','full_load'])

#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    print("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)
    



#for TempDir USE BASE PATH
args['raw_db_name']='build_lab'
#args['raw_db_name']='d11_cost_insights_v2'
args['raw_table_name']='processed_dt_insights_fact'
# args['full_load']='false'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


db_table="acc_cost_v2.data_transfer_insights_fact"
db_name='prodinsightsdb'
stag_db_table='acc_cost_v2.stage_data_transfer_insights_fact'






logger.info("****using following args****")
logger.info(str(args))
logger.info("****using following args****")



datasource0 = glueContext.create_dynamic_frame_from_catalog(
    database=args['raw_db_name'],
      table_name=args['raw_table_name'])

df=datasource0.toDF()

if args['full_load']!='true':
    df=df.filter((df.month==args['month']) & (df.year==args['year']))
    
df=df.drop("month","year")


pre_query = "begin; "\
        "drop table if exists acc_cost_v2.stage_data_transfer_insights_fact; "\
        "create table acc_cost_v2.stage_data_transfer_insights_fact as "\
        "select * from acc_cost_v2.data_transfer_insights_fact "\
        "where 1=2; "\
        "end;"
    
post_query = "begin; delete from acc_cost_v2.data_transfer_insights_fact using acc_cost_v2.stage_data_transfer_insights_fact where acc_cost_v2.stage_data_transfer_insights_fact.date_dim_id =acc_cost_v2.data_transfer_insights_fact.date_dim_id and acc_cost_v2.stage_data_transfer_insights_fact.linked_account_id =acc_cost_v2.data_transfer_insights_fact.linked_account_id ; insert into acc_cost_v2.data_transfer_insights_fact select * from acc_cost_v2.stage_data_transfer_insights_fact; end;"
       

dyf = DynamicFrame.fromDF(df, glueContext, "dyf")


datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "acc_redshift", connection_options = {"dbtable": stag_db_table, "database":db_name,"preactions":pre_query,"postactions":post_query}, redshift_tmp_dir = args["TempDir"]+"/dt_insights_fact", transformation_ctx = "datasink4")
job.commit()
