import awswrangler as wr
from awsglue.utils import getResolvedOptions
import datetime
import pytz
import sys
import hashlib
import numpy as np

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
# args['delta']='true'
# args['full_load']='false'



args['raw_table']='summary_view'
args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'


args['processed_table']='processed_tag_dim'
args['processed_database']='d11_cost_insights_v2'
#args['processed_database']='build_lab'

# args['s3_output_path']='s3://acc-buildapp-test/processed/dimensions/tag_dim/'

def create_sha(athena_df):
    
    
    athena_df = athena_df.reindex(sorted(athena_df.columns), axis=1)
    athena_df_colms=athena_df.columns
    athena_df['concated_clms']=athena_df[athena_df_colms].apply(lambda row: '||'.join(row.values.astype(str)), axis=1)
    athena_df['tag_dim_id'] = athena_df['concated_clms'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    
    
    
    athena_df=athena_df.drop("concated_clms",axis=1)
    return athena_df





wr_df=wr.catalog.table(database=args['raw_database'], table=args['raw_table'])

colnm=wr_df[wr_df['Column Name'].str.contains("resource_tags_user_")]

colnm_list=colnm['Column Name'].values.tolist()
colnm_list.sort()


#colnm_list_1=[ "Coalesce(nullif("+s+",''),'__NA__') as "+s for s in colnm_list ]






colnm_str=','.join(colnm_list)
#colnm_str_1=','.join(colnm_list_1)
print("colnm_str:",colnm_str)


if  args['delta']=='true':
    
    
    athena_df_raw=wr.athena.read_sql_query(''' select {} 
    from 
    {} where month='{}' and year='{}'  group by {}'''
    .format(colnm_str,args['raw_table'],args['month'],args['year'],colnm_str), database=args['raw_database'])
    
    athena_df_raw=create_sha(athena_df_raw)
   
    
    
    
    
    athena_df_pro=wr.athena.read_sql_query(''' select * 
    from {} '''
    .format(args['processed_table']), database=args['processed_database'])
    
    
    common = athena_df_raw.merge(athena_df_pro, on="tag_dim_id",how="left",indicator=True)
    
    
    
    athena_df=common[common['_merge']=="left_only"]
    
    
    
    
    athena_df=athena_df.drop("_merge",axis=1)
    
    athena_df=create_sha(athena_df)
    
    if not athena_df.empty:
    
        wr.s3.to_parquet(
        df=athena_df,
        path=args['s3_output_path'],
        dataset=True,
        mode="append"
        )
    else:
        print("empty athena_df")
    
  
else:
    if args['full_load']=='false':
        athena_df=wr.athena.read_sql_query(''' SELECT {}
        FROM {} where month='{}' and year='{}'
        GROUP BY  {}
        '''.format(colnm_str,args['raw_table'],args['month'],args['year'],colnm_str), database=args['raw_database'])


        athena_df=create_sha(athena_df)
        if not athena_df.empty:
    
            wr.s3.to_parquet(
            df=athena_df,
            path=args['s3_output_path'],
            dataset=True,
            mode="append"
            )
        else:
            print("empty athena_df")
    else:
        print("in full load true")
        
        wr.s3.delete_objects(args['s3_output_path'])
        athena_df=wr.athena.read_sql_query(''' SELECT {}
        FROM {}
        GROUP BY  {}
        '''.format(colnm_str,args['raw_table'],colnm_str), database=args['raw_database'],chunksize=True)
        
        for df in athena_df:
            df=create_sha(df)
            if not df.empty:
    
                wr.s3.to_parquet(
                df=df,
                path=args['s3_output_path'],
                dataset=True,
                mode="append"
                )
            else:
                print("empty athena_df")
            


    

























