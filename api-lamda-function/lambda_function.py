import json
import boto3
import pandas as pd
import pyarrow.parquet as pq
import awswrangler as wr

def lambda_handler(event, context):
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    # Specify the S3 bucket and object key
    s3_path = ' '
    
    try:
        
        street_block_= event['street_block_name']
        
        df = wr.s3.read_parquet(path=s3_path)
        
        # Process the data or perform any desired operations
        df = df[df['street_block'] == street_block_]
        
        # Drop the unwanted columns
        df = df.drop(['total_gross_amount', 'total_transactions'], axis=1)
        
        # Rename the 'label_category' column to 'traffic_cluster'
        df = df.rename(columns={'label_category': 'traffic_cluster'})
        
        # Return a response or result if needed
        return {
            'statusCode': 200,
            'body': df.to_dict(orient='records')
        }
        
    except Exception as e:
        # Handle any errors that occur during the process
        return {
            'statusCode': 500,
            'body': 'Error reading or processing data: ' + str(e)
        }
