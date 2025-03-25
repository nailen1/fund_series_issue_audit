from mongodb_controller import client
from financial_dataset_preprocessor import get_mapping_menu8186, get_fund_codes_main, get_mapping_fund_names
# db = client['database-rpa']
# collection_menu8186 = db['dataset-menu8186']

# pipeline = [
#     {
#         '$match': {
#             '자산': '국내주식'
#         }
#     },
#     {
#         '$project': {
#             '_id': 0,
#             '일자': 1,
#             '종목': 1,
#             '종목명': 1,
#             '비중': 1
#         }
#     }
# ]


import requests
import json

def fetch_response_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    
    except requests.exceptions.RequestException as e:
        print(f"Error occurred during request: {e}")
        return None

def get_data_from_response(response):
    if response is None:
        print("No response available.")
        return None
    
    try:
        data = response.json()
        return data
    
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        return None
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

def get_mapping_fund_names():
    fund_mapping_url = "http://127.0.0.1:8080/api/fundcode/mapping/all/"
    response = fetch_response_from_url(fund_mapping_url)
    mapping = get_data_from_response(response)
    return mapping

def get_mapping_inception_dates():
    return get_mapping_menu8186(col_for_range='설정일', date_ref=None)

FUND_CODES_MAIN = get_fund_codes_main(date_ref=None)
MAPPING_FUND_NAMES = get_mapping_fund_names()
MAPPING_INCEPTION_DATES = get_mapping_inception_dates()
