# import psycopg2
# from psycopg2.extras import RealDictCursor
import json
import os
from datetime import datetime

# Initialize the RDS Data API client
client = boto3.client('rds-data')


DB_CLUSTER_ARN = 'arn:aws:rds:region:account-id:cluster:db-cluster-name'
SECRET_ARN = 'arn:aws:secretsmanager:region:account-id:secret:your-db-secret'
DATABASE_NAME = 'your-database-name'

def handler(event, context):
    # conn = psycopg2.connect(
    #     host=os.environ['DB_HOST'],
    #     database=os.environ['DB_NAME'],
    #     user=os.environ['DB_USER'],
    #     password=os.environ['DB_PASS'],
    #     port=5432,
    # )
    sql_query = "SELECT * FROM pim_prod.brand limit 10"
     response = client.execute_statement(
            resourceArn=DB_CLUSTER_ARN,
            secretArn=SECRET_ARN,
            database=DATABASE_NAME,
            sql=sql_query
        )
 # Process the results
    records = response['records']
    result = []
    for record in records:
            result.append({
                'column1': record[0]['stringValue'],
                'column2': record[1]['stringValue']
            })
        
     # Return results
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        # Close database connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def serialize_value(value):
    """Convert value to a JSON-serializable format."""
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to string
    return value  # Return other values as is

# handler()