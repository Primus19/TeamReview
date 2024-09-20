import pandas as pd
import io
import json
import requests
import base64
import boto3
import os
from datetime import datetime

s3 = boto3.client('s3')

# Get Confluence settings from environment variables
confluence_base_url = os.environ.get('CONFLUENCE_BASE_URL')
confluence_page_id = os.environ.get('CONFLUENCE_PAGE_ID')
confluence_username = os.environ.get('CONFLUENCE_USERNAME')
confluence_api_token = os.environ.get('CONFLUENCE_API_TOKEN')

def lambda_handler(event, context):
    try:
        # Process the S3 event
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        adjusted_key = s3_key.replace('20240725', '20240722')  # Adjust the date in the key
        
        # Read CSV from S3
        response = s3.get_object(Bucket=s3_bucket, Key=adjusted_key)
        df = pd.read_csv(response['Body'])

        # Check for required columns
        required_columns = ['db_instance_identifier', 'db_name', 'db_instance_class', 'engine', 'engine_version', 'instance_create_time', 'last_updated']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            raise Exception(f"Missing columns: {', '.join(missing_columns)}")

        # Compute DB Age
        df['instance_create_time'] = pd.to_datetime(df['instance_create_time'], errors='coerce')

        # Ensure timezone-naive datetimes for comparison
        df['instance_create_time'] = df['instance_create_time'].dt.tz_localize(None)
        now = pd.Timestamp.now().tz_localize(None)

        df['db_age_days'] = (now - df['instance_create_time']).dt.days

        # Filter data for instances older than 90 days
        df = df[df['db_age_days'] > 90]

        # Remove duplicates from the entire dataframe
        df = df.drop_duplicates()

        # Ensure the engine_version column is sorted correctly
        df['engine_version'] = df['engine_version'].apply(lambda x: tuple(map(int, x.split('.'))))  # Convert versions to tuples for comparison
        df = df.sort_values(by='engine_version', ascending=True)  # Sort by engine version in ascending order

        # Separate reports and include additional columns
        rds_instance_status_details = df[['db_instance_identifier', 'db_name', 'engine', 'db_age_days', 'engine_version']]

        # Save CSV to S3
        save_csv_to_s3(rds_instance_status_details, 'output/status_rds_instances/rds_instance_status_details.csv')

        # Format content for Confluence
        rds_report = format_rds_report(rds_instance_status_details)

        # Get current page content and version
        current_version = get_current_page_version(confluence_page_id)

        # Build new content
        updated_content = build_updated_content(rds_report)

        # Push updated content to Confluence
        push_to_confluence(updated_content, current_version)

        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete.')
        }

    except Exception as e:
        print(f"Exception encountered: {e}")

        # Create an error report for Confluence
        error_report = f"""
        <h3 style="color:#e74c3c;">Error Report</h3>
        <p style="color:#34495e;">An error occurred during the processing of the data:</p>
        <p style="color:#e74c3c;">{str(e)}</p>
        """

        # Retrieve current page version
        try:
            current_version = get_current_page_version(confluence_page_id)
            updated_content = build_updated_content(error_report)
            push_to_confluence(updated_content, current_version)
        except Exception as confluence_error:
            print(f"Exception encountered while updating Confluence: {confluence_error}")

        return {
            'statusCode': 500,
            'body': json.dumps(f'Failed: {str(e)}')
        }

def get_current_page_version(page_id):
    url = f"{confluence_base_url}/{page_id}?expand=version"
    headers = {
        'Authorization': f'Basic {base64.b64encode(f"{confluence_username}:{confluence_api_token}".encode()).decode()}'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        page_data = response.json()
        version = page_data['version']['number']
        return version
    else:
        raise Exception(f"Failed to fetch current version of Confluence page: {response.text}")

def save_csv_to_s3(df, s3_key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket='gov-configuration-data', Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Saved CSV to S3: {s3_key}")

def push_to_confluence(content, current_version):
    url = f"{confluence_base_url}/{confluence_page_id}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {base64.b64encode(f"{confluence_username}:{confluence_api_token}".encode()).decode()}'
    }
    
    data = {
        'version': {'number': current_version + 1},
        'title': 'RDS Instance Status Report',
        'type': 'page',
        'body': {'storage': {'value': content, 'representation': 'storage'}}
    }

    try:
        response = requests.put(url, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            print("Successfully updated Confluence page.")
        else:
            print(f"Failed to update Confluence page. Response: {response.text}")

    except Exception as e:
        print(f"Exception encountered while updating Confluence: {e}")
        raise

def build_updated_content(rds_report):
    # Build new content with the RDS instance report
    updated_content = f"""
    <h2 style="color:#1f78d1;">RDS Instance Report</h2>
    <h3 style="color:#2c3e50;">RDS Instance Status Details</h3>
    {rds_report}
    """
    return updated_content

def format_rds_report(df):
    html_content = '''
    <p>This report provides details on RDS instances with a focus on their status, age, and version.</p>
    <table style="width:100%; border-collapse:collapse; border:1px solid #ddd; font-family: Arial, sans-serif;">
        <thead style="background-color:#f4f4f4;">
            <tr>
                <th style="border:1px solid #ddd; padding:12px; text-align:left; background-color:#e0e0e0; color:#333;"><b>DB Instance Identifier</b></th>
                <th style="border:1px solid #ddd; padding:12px; text-align:left; background-color:#e0e0e0; color:#333;"><b>DB Name</b></th>
                <th style="border:1px solid #ddd; padding:12px; text-align:left; background-color:#e0e0e0; color:#333;"><b>Engine</b></th>
                <th style="border:1px solid #ddd; padding:12px; text-align:left; background-color:#e0e0e0; color:#333;"><b>DB Age in Days</b></th>
                <th style="border:1px solid #ddd; padding:12px; text-align:left; background-color:#e0e0e0; color:#333;"><b>Engine Version</b></th>
            </tr>
        </thead>
        <tbody>
    '''

    for _, row in df.iterrows():
        html_content += '<tr>'
        html_content += f'<td style="border:1px solid #ddd; padding:12px; text-align:left;">{row["db_instance_identifier"] if pd.notna(row["db_instance_identifier"]) else ""}</td>'
        html_content += f'<td style="border:1px solid #ddd; padding:12px; text-align:left;">{row["db_name"] if pd.notna(row["db_name"]) else ""}</td>'
        html_content += f'<td style="border:1px solid #ddd; padding:12px; text-align:left;">{row["engine"] if pd.notna(row["engine"]) else ""}</td>'
        html_content += f'<td style="border:1px solid #ddd; padding:12px; text-align:left;">{row["db_age_days"] if pd.notna(row["db_age_days"]) else ""}</td>'
        html_content += f'<td style="border:1px solid #ddd; padding:12px; text-align:left;">{".".join &#8203;:contentReference[oaicite:0]{index=0}&#8203;
