import psycopg2
import json
import os
from datetime import datetime

def handler(event, context):
    conn = psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        port=5432,
    )
    cursor = conn.cursor()
    try:
        # Example query
        cursor.execute("SELECT * FROM pim_prod.import_bundle limit 15")
        rows = cursor.fetchall()
        # Get the column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Convert rows into a list of dictionaries
        response_data = []
        for row in rows:
            formatted_row = {col_name: serialize_value(item) for col_name, item in zip(column_names, row)}
            response_data.append(formatted_row)
        print('Response Data:', response_data)
        return {
            'statusCode': 200,
            'headers': {
            'Access-Control-Allow-Origin': '*', 
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE',
            'Access-Control-Allow-Headers': 'Content-Type',
            },
            'body': json.dumps(response_data)  # Convert to JSON
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