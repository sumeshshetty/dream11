import awswrangler as wr
from awsglue.utils import getResolvedOptions
import datetime
import pytz
import sys
import hashlib
import pandas as pd
tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.datetime.now(tz)



#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','delta','full_load','s3_output_path'])

#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    print("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)
# args['delta']='false'
# args['full_load']='true'

# args['raw_table']='raw_cost_usage_hourly_athena'
# args['raw_table_second']='raw_cur_data_sporta_management'

args['raw_table']='summary_view'
args['raw_table_second']='summary_view_second'

args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'
args['processed_table']='processed_resource_dim'
args['processed_database']='d11_cost_insights_v2'
#args['processed_database']='build_lab'
# args['s3_output_path']='s3://acc-buildapp-test/processed/dimensions/resource_dim/'
print("################## args ###########")
print(args)
print("################## args ###########")
def create_sha(athena_df):
    
    
    athena_df = athena_df.reindex(sorted(athena_df.columns), axis=1)
    athena_df_colms=athena_df.columns
    
    
    
    athena_df['concated_clms']=athena_df[athena_df_colms].apply(lambda row: '||'.join(row.values.astype(str)), axis=1)
    athena_df['aws_product_dim_id'] = athena_df['concated_clms'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    
    
    
    athena_df=athena_df.drop("concated_clms",axis=1)
    return athena_df


if args['delta']=='true':
    # athena_df=wr.athena.read_sql_query(f''' SELECT  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
    # from
    # {args['raw_table']} where month='{args['month']}' and year='{args['year']}' and product_sku
    # not in (select aws_product_dim_id from {args['processed_table']} ) GROUP BY  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
    # ''', database=args['raw_database'])
    
    # athena_df=create_sha(athena_df)
    
    # if (athena_df.shape[0]) > 0:
    #     wr.s3.to_parquet(
    #     df=athena_df,
    #     path=args['s3_output_path'],
    #     dataset=True,
    #     mode="append")
    # else:
    #     print("error writing to s3")
    print("delta true....")
    athena_df_raw=wr.athena.read_sql_query(f''' SELECT  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
    from
    {args['raw_table']} where month='{args['month']}' and year='{args['year']}' group by product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code ''', database=args['raw_database'],chunksize=True)
    
    athena_df_raw_list=[]
    for df in athena_df_raw:
        
        df=create_sha(df)
        athena_df_raw_list.append(df)
       
    
    athena_df_raw_concat=pd.concat(athena_df_raw_list)
    print("athena_df_raw_concat:",athena_df_raw_concat.shape[0])
    
    athena_df_pro=wr.athena.read_sql_query(f''' SELECT * FROM {args['processed_table']} ''', database=args['processed_database'])
    
    
    print("athena_df_pro:",athena_df_pro.shape[0])
    
    
    common = athena_df_raw_concat.merge(athena_df_pro, on="aws_product_dim_id",how="left",indicator=True)
    print("common",common.shape[0])
    print(common['aws_product_dim_id'].head(5))
    athena_df=common[common['_merge']=="left_only"]
    athena_df=athena_df.drop("_merge",axis=1)
    print("final",athena_df.shape[0])
    # if (athena_df.shape[0]) > 0:
    #     wr.s3.to_parquet(
    #     df=athena_df,
    #     path=args['s3_output_path'],
    #     dataset=True,
    #     mode="append")
    # else:
    #     print("error writing to s3")
        

else:
    if args['full_load']=='false':
        athena_df=wr.athena.read_sql_query(f''' SELECT  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code 
        FROM {args['raw_table']} where month='{args['month']}' and year='{args['year']}' GROUP BY  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        ''', database=args['raw_database'])
        
        athena_df_second=wr.athena.read_sql_query(f''' SELECT  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        FROM {args['raw_table_processed']} where month='{args['month']}' and year='{args['year']}' GROUP BY  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        ''', database=args['raw_database'])
        
        
        
        athena_df=athena_df.append(athena_df_second)
        
        athena_df=create_sha(athena_df)
        athena_df.drop_duplicates(keep='first',inplace=True)
        if (athena_df.shape[0]) > 0:
            wr.s3.to_parquet(
            df=athena_df,
            path=args['s3_output_path'],
            dataset=True,
            mode="append")
        else:
            print("error writing to s3")
    else:
        wr.s3.delete_objects(args['s3_output_path'])
        
        athena_df=wr.athena.read_sql_query(f''' SELECT  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        FROM {args['raw_table']}
        GROUP BY  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        ''', database=args['raw_database'],chunksize=True)
        
        
        athena_df_second=wr.athena.read_sql_query(f''' SELECT  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        FROM {args['raw_table_second']}
        GROUP BY  product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
        ''', database=args['raw_database'],chunksize=True)
        
        athena_df_raw_list=[]
        for df in athena_df:
            
            df=create_sha(df)
            athena_df_raw_list.append(df)
        
        athena_df_concat=pd.concat(athena_df_raw_list)
        
        athena_df_second_list=[]
        for df2 in athena_df_second:
            
            df2=create_sha(df2)
            athena_df_second_list.append(df2)
        
        athena_df_second_concat=pd.concat(athena_df_second_list)
        
        
        df=athena_df_concat.append(athena_df_second_concat)
        #df=athena_df_concat
        
        df.drop_duplicates(keep='first',inplace=True)
        
        if (df.shape[0]) > 0:
            print("puutinf to s3..")
            wr.s3.to_parquet(
            df=df,
            path=args['s3_output_path'],
            dataset=True,
            mode="append")
        else:
            print("error writing to s3")
        
        # for df in athena_df:
        #     df=create_sha(df)
            
        #     if (df.shape[0]) > 0:
        #         print("puutinf to s3..")
        #         wr.s3.to_parquet(
        #         df=df,
        #         path=args['s3_output_path'],
        #         dataset=True,
        #         mode="append")
        #     else:
        #         print("error writing to s3")
        