import awswrangler as wr
from awsglue.utils import getResolvedOptions

import pytz
import sys
import boto3
from datetime import datetime,timedelta
#import datetime
import botocore
from dateutil.tz import tzlocal
request_arn='arn:aws:iam::476924516882:role/assume-role-to-proserv-account'

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)
org_client= boto3.client('organizations')



#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_output_path','full_load','delta'])


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


def get_account_name_re(line_item_usage_account_id):
    
    session = assumed_role_session(request_arn)
    org_client= session.client('organizations')
    
    try:
        response = org_client.describe_account(
        AccountId=line_item_usage_account_id
    )
        
        return response['Account']['Name']
    except Exception as e:
        print(f"checking account...: {line_item_usage_account_id}")
        print(f"exception {e}")
        return ''


#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    #logger.info("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)
# args['delta']='true'
# args['full_load']='false'
 

args['raw_table']='raw_cost_usage_hourly_athena'
args['raw_table_second']='raw_cur_data_sporta_management'

args['raw_database']='d11_cost_insights_v2'
#args['raw_database']='build_lab'
args['processed_table']='processed_account_dim'
args['processed_database']='d11_cost_insights_v2'
#args['processed_database']='build_lab'
#args['s3_output_path']='s3://acc-buildapp-test/processed/dimensions/account_dim/'

def get_account_name(line_item_usage_account_id):
    
    
    try:
        response = org_client.describe_account(
        AccountId=line_item_usage_account_id
    )
        print(f"for {line_item_usage_account_id} got {response['Account']['Name']}")
        return response['Account']['Name']
    except Exception as e:
        
        print(f"re-checking account...: {line_item_usage_account_id}")
        
        account_name=get_account_name_re(line_item_usage_account_id)
        print(f"for rechecked1 {line_item_usage_account_id} got {account_name}")
        
        
        if account_name:
            return account_name
        else:
            return ''


mode='append'

if  args['delta']=='true':
    
    
    athena_df=wr.athena.read_sql_query(f''' select bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id as aws_account_dim_id from 
    {args['raw_table']} where month='{args['month']}' and year='{args['year']}' and line_item_usage_account_id  
    not in (select line_item_usage_account_id from {args['processed_table']} ) group by bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
  ''', database=args['raw_database'])
  
    athena_df_second=wr.athena.read_sql_query(f''' select bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id as aws_account_dim_id from 
    {args['raw_table_second']} where month='{args['month']}' and year='{args['year']}' and line_item_usage_account_id  
    not in (select line_item_usage_account_id from {args['processed_table']} ) group by bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
  ''', database=args['raw_database'])
  
    athena_df=athena_df.append(athena_df_second)
    
  
  
    
  
  
    
    
  
else:
    if args['full_load']=='false':
        athena_df=wr.athena.read_sql_query(f''' SELECT bill_payer_account_id, 
                line_item_usage_account_id,
                line_item_usage_account_id AS aws_account_dim_id
        FROM {args['raw_table']} where month='{args['month']}' and year='{args['year']}'
        GROUP BY  bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
        ''', database=args['raw_database'])
        
        athena_df_second=wr.athena.read_sql_query(f''' SELECT bill_payer_account_id, 
                line_item_usage_account_id,
                line_item_usage_account_id AS aws_account_dim_id
        FROM {args['raw_table_second']} where month='{args['month']}' and year='{args['year']}'
        GROUP BY  bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
        ''', database=args['raw_database'])
        
        athena_df=athena_df.append(athena_df_second)


    else:
        mode='overwrite'
        athena_df=wr.athena.read_sql_query(f''' SELECT bill_payer_account_id, 
        line_item_usage_account_id,
        line_item_usage_account_id AS aws_account_dim_id
        FROM {args['raw_table']}
        GROUP BY  bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
        ''', database=args['raw_database'])
        
        athena_df_second=wr.athena.read_sql_query(f''' SELECT bill_payer_account_id, 
        line_item_usage_account_id,
        line_item_usage_account_id AS aws_account_dim_id
        FROM {args['raw_table_second']}
        GROUP BY  bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
        ''', database=args['raw_database'])
        
        athena_df=athena_df.append(athena_df_second)
        
athena_df.drop_duplicates(keep='first',inplace=True)
athena_df['account_name'] = athena_df['line_item_usage_account_id'].apply(get_account_name)



if (athena_df.shape[0]) > 0:
    wr.s3.to_parquet(
    df=athena_df,
    path=args['s3_output_path'],
    dataset=True,
    mode=mode
    )
else:
    print("no new records found")
      
    
        
    
    
    
        
    
        
    
