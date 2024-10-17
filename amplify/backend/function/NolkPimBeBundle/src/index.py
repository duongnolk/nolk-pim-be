import psycopg2
import json
import os
from datetime import datetime
import math


def getConnection():
    return psycopg2.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        port=5432,
    )


def handler(event, context):
    query_params = event.get('queryStringParameters', {})
    path = event.get('path', '/')
    body = event.get('body', {})
    print(f"Path: {path}")
    print(f"Query Parameters: {query_params}")
    print(f"Body: {body}")

    if path == '/bundle/filter':
        return filter(query_params)


def serialize_value(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')  # Convert datetime to string
    return value  # Return other values as is


def pageable(page_number, total_pages, rows, total_records, response_data, has_more):
    response = {
        'currentPage': page_number,
        'totalPages': total_pages,
        'recordsOnPage': len(rows),
        'totalRecords': total_records,
        'records': response_data,
        'hasMore': has_more
    }
    return response


def filter(params):
    query_params = []
    count_params = []
    try:
        # Parameters passed from API Gateway
        page_number = int(params.get('pageNumber', 1))  # Default to page 1
        records_per_page = int(params.get('recordsPerPage', 10))  # Default 10 per page
        offset = (page_number - 1) * records_per_page

    except(ValueError, KeyError) as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f"Invalid input: {str(e)}"),
            'headers': {'Content-Type': 'application/json'}
        }

    conn = getConnection()
    try:
        with conn.cursor() as cursor:
            countq = "SELECT COUNT(*) FROM pim_prod.import_bundle where TRUE"
            # Fetch the paginated records
            query = "SELECT * FROM pim_prod.import_bundle where TRUE"

            parent_sku = params.get('parent_sku', None)

            # Add filters dynamically
            if parent_sku:
                query += " AND parent_sku LIKE %s"
                countq += " AND parent_sku LIKE %s"
                query_params.append(f"%{parent_sku}%")
                count_params.append(f"%{parent_sku}%")

            # Fetch total number of records
            cursor.execute(countq, count_params)
            total_records = cursor.fetchone()[0]

            # Calculate total pages
            total_pages = math.ceil(total_records / records_per_page)

            # If page number exceeds the total pages, return an empty result
            if page_number > total_pages:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'currentPage': page_number,
                        'totalPages': total_pages,
                        'recordsOnPage': 0,
                        'totalRecords': total_records,
                        'records': [],
                        'hasMore': False
                    }),
                    'headers': {'Content-Type': 'application/json'}
                }

            # Add pagination to the query
            query += " ORDER BY parent_sku LIMIT %s OFFSET %s"
            query_params.append(records_per_page)
            query_params.append(offset)
            cursor.execute(query, query_params)
            records = cursor.fetchall()

        # Get the column names from cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Convert rows into a list of dictionaries
        response_data = []
        for row in records:
            formatted_row = {col_name: serialize_value(item) for col_name, item in zip(column_names, row)}
            response_data.append(formatted_row)

        # Check if there are more pages
        has_more = page_number < total_pages
        print('Response Data:',
              json.dumps(pageable(page_number, total_pages, records, total_records, response_data, has_more)))
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE',
                'Access-Control-Allow-Headers': 'Content-Type',
            },
            'body': json.dumps(pageable(page_number, total_pages, records, total_records, response_data, has_more))
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()