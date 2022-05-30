import awswrangler as wr
import pandas as pd
from datetime import datetime,timedelta
import pytz
from awsglue.utils import getResolvedOptions
import sys

tz=pytz.timezone("Asia/Calcutta")


#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME','s3_output_path','full_load'])


# args['s3_output_path']='s3://acc-buildapp-test/processed/dimensions/date_dim/'
# args['full_load']='false'
args['raw_table']='raw_cost_usage_hourly_athena'
args['raw_database']='d11_cost_insights_v2'
args['processed_database']='d11_cost_insights_v2'
args['processed_table']='processed_date_dim'


print("script running with following args")
print(args)
print("script running with following args")


def get_formatted_df(date1):
    year1=str(date1.year)
    
    month1=str(date1.month)
    if len(month1)==1:
        month1="0"+month1
    
    day1=str(date1.day)
    if len(day1)==1:
        day1="0"+day1
    
    date_dim_id=year1+month1+day1
    df = pd.DataFrame({"date_dim_id":[date_dim_id] , "day_attr": [day1], "month_attr": [month1], "year_attr": [year1]})
    return df
    
mode='append'  
if args['full_load']=='true':
    
    mode='overwrite'  
    athena_df=wr.athena.read_sql_query(f''' select cast(min(line_item_usage_start_date) as date) as min_date,cast(max(line_item_usage_start_date) as date) as max_date  from raw_cost_usage_hourly_athena ''',
    database=args['raw_database'])
    
    min_date=athena_df['min_date'][0]
    max_date=athena_df['max_date'][0]
    
    print("min_date",min_date)
    print("max_date",max_date)
    
    min_date=datetime.strptime(str(min_date),'%Y-%m-%d')
    max_date=datetime.strptime(str(max_date),'%Y-%m-%d')
    
    df=get_formatted_df(min_date)
    
    
    while True:
        min_date=min_date+timedelta(days=1)
        df_1=get_formatted_df(min_date)
        df=df.append(df_1)
        
        
        if min_date>max_date:
            break
    
else:
    
    
    now=datetime.now(tz)
    df=get_formatted_df(now)
    
    
    date_dim_id_df=wr.athena.read_sql_query(f''' select date_dim_id  from {args['processed_table']} where date_dim_id='{df['date_dim_id'][0]}'  ''',
    database=args['processed_database'])
    
    
    
    if not date_dim_id_df.empty:
        print("exiting ....")
        sys.exit(0)
        
    
    

try:
    wr.s3.to_parquet(
        df=df,
        path=args['s3_output_path'],
        mode=mode,
        dataset=True
    )
    
    print("successfully written to s3")
except Exception as e:
    print("eror writing to s3")
    print(e)

