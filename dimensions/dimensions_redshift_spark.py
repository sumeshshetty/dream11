import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


logger = glueContext.get_logger()





#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','TempDir','s3_input_path_date_dim','s3_input_path_resource_dim','s3_input_path_account_dim','s3_input_path_tag_dim'])
job.init(args['JOB_NAME'], args)

args['raw_table']='processed_tag_dim'
args['raw_database']='d11_cost_insights_v2'

# args['s3_input_path_date_dim']='s3://acc-buildapp-test/processed/date_dim'
dbtable_date_dim='acc_cost_v2.date_dim'

# args['s3_input_path_resource_dim']='s3://acc-buildapp-test/processed/resource_dim'
dbtable_resource_dim='acc_cost_v2.resource_dim'

# args['s3_input_path_account_dim']='s3://acc-buildapp-test/processed/account_dim'
dbtable_account_dim='acc_cost_v2.account_dim'

# args['s3_input_path_tag_dim']='s3://acc-buildapp-test/processed/tag_dim'

#dbtable_tag_dim='acc_cost_v2.tag_dim'
dbtable_tag_dim='acc_cost_v2.tag_dim'
db_name='prodinsightsdb'
# new_dbtable_tag_dim="acc_cost_v2.new_tag_dim"
new_dbtable_tag_dim="acc_cost_v2.new_tag_dim"



datasource_date_dim = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_date_dim']]
      }, 
      format="parquet", 
      transformation_ctx="datasource_date_dim")




datasource_resource_dim = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_resource_dim']]
      }, 
      format="parquet", 
      transformation_ctx="datasource_resource_dim")



datasource_account_dim = glueContext.create_dynamic_frame_from_options(
      connection_type="s3", 
      connection_options = {
        "paths": [args['s3_input_path_account_dim']]
      }, 
      format="parquet", 
      transformation_ctx="datasource_account_dim")




# datasource_tag_dim = glueContext.create_dynamic_frame_from_options(
#       connection_type="s3", 
#       connection_options = {
#         "paths": [args['s3_input_path_tag_dim']]
#       }, 
#       format="parquet",
#       additional_options={"mergeSchema": "true"}
#       )
datasource_tag_dim = glueContext.create_dynamic_frame.from_catalog(database = args['raw_database'], table_name = args['raw_table'], transformation_ctx = "datasource_tag_dim",additional_options={"mergeSchema": "true"} )



datasource_tag_dim.show(5)


dbtable_tag_dim_ws=dbtable_tag_dim.split(".")[1]


post_query=f"begin; drop table if exists {dbtable_tag_dim}; alter table {new_dbtable_tag_dim} rename to {dbtable_tag_dim_ws}; grant select on {dbtable_tag_dim} to looker_admin; end;"
datasink_tag_dim = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource_tag_dim,
                catalog_connection = "acc_redshift", 
                 connection_options ={"dbtable": new_dbtable_tag_dim, "database":db_name ,"postactions":post_query}, 
                redshift_tmp_dir = args["TempDir"]+"/tag_dim", transformation_ctx = "datasink_tag_dim")

            
pre_query_resource=f"begin; truncate table acc_cost_v2.resource_dim; end;"
datasink_resource_dim = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource_resource_dim,
                catalog_connection = "acc_redshift", 
                 connection_options ={"dbtable": dbtable_resource_dim, "database":db_name ,"preactions":pre_query_resource}, 
                redshift_tmp_dir = args["TempDir"]+"/resource_dim", transformation_ctx = "datasink_resource_dim")

pre_query_account=f"begin; truncate table acc_cost_v2.account_dim; end;"
datasink_account_dim = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource_account_dim,
                catalog_connection = "acc_redshift", 
                 connection_options ={"dbtable": dbtable_account_dim, "database":db_name ,"preactions":pre_query_account}, 
                redshift_tmp_dir = args["TempDir"]+"/account_dim", transformation_ctx = "datasink_account_dim")
  
pre_query_date=f"begin; truncate table acc_cost_v2.date_dim; end;"           
datasink_date_dim = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource_date_dim,
                catalog_connection = "acc_redshift", 
                 connection_options ={"dbtable": dbtable_date_dim, "database":db_name ,"preactions":pre_query_date}, 
                redshift_tmp_dir = args["TempDir"]+"/date_dim", transformation_ctx = "datasink_date_dim")


job.commit()
logger.info("***succesfull execution**")



