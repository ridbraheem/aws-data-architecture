import json
import boto3
from datetime import datetime, timedelta,date
#from botocore.vendored import requests
import requests

today = date.today()
s3 = boto3.client('s3')
s3_bucket = ''

# Subtract 14 days from the current date
two_weeks_ago = today - timedelta(days=14)
two_weeks_ago = str(two_weeks_ago) + 'T00:00:00.000'
    
def main():
    
    params = {
        "$limit": 10000000
    }
    response = requests.get(f'''https://data.sfgov.org/resource/imvp-dq3v.json?$where=session_end_dt > '{two_weeks_ago}' ''', params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"Retrieved {len(data)} records from the dataset!")
    else:
        print("Failed to retrieve data from the dataset!")
    
    #data = json.dumps({'test':'test'}, indent = 2)
    
    #FileName = 'sf-parking/raw/parking_meters_transactions/parking_meters_transactions' + str(datetime.now()) + '.json'
    
    FileName = 'sf-parking/raw/parking_meters_transactions/parking_meters_transactions' + '.json'
    
    data = '\n'.join([json.dumps(d) for d in data])
    
    s3.put_object(Bucket=s3_bucket, Body=data, Key=FileName)
    
if __name__ == "__main__":
    main()
