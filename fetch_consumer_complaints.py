import requests
from datetime import datetime, timedelta
import json

api_endpoint = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
page_size = 1000

def fetch_complaints(start_date, end_date):
    params = {
        'date_received_min': start_date,
        'date_received_max': end_date,
        'product': 'Credit card',
        'size': page_size
    }
    
    complaints = []
    page = 0
    max_frm_value = 10000 - page_size

    while True:
        params['frm'] = page * page_size

        if params['frm'] > max_frm_value:
            break

        try:
            response = requests.get(api_endpoint, params=params)
            response.raise_for_status()
            
            if response.status_code == 200:
                data = response.json()
                batch = data['hits']['hits']
                complaints.extend(batch)

                if len(batch) < page_size:
                    break

            page += 1
        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            break

    return complaints

def lambda_handler(event, context):
    try:
        all_complaints = []
        start_date = datetime(2023, 9, 1)
        end_date = datetime.now()

        while start_date < end_date:
            next_month = start_date + timedelta(days=32)
            next_month = next_month.replace(day=1)

            complaints = fetch_complaints(start_date.strftime('%Y-%m-%d'), next_month.strftime('%Y-%m-%d'))
            all_complaints.extend(complaints)
            print("Fetched some complaints")

            start_date = next_month

        complaints_data = [complaint['_source'] for complaint in all_complaints]
        print(complaints_data[1])
        processed_count = len(complaints_data)
        return {
            'statusCode': 200,
            'body': json.dumps({"message": "Complaints data processed", "count": processed_count})
        }
    except Exception as e:
        print(f"An error occurred in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal server error'}),
        }