import json
import sys

import pandas_gbq
from py5paisa import FivePaisaClient
import configparser
import logging.handlers
import requests
import pandas as pd
import datetime
from tabulate import tabulate
import re,os
import csv
from utils import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.max_rows', None)     # Show all rows
pd.set_option('display.width', 1000)


def convert_date(date_string):
    match = re.search(r'\d+', date_string)
    if match:
        timestamp = int(match.group())
        return pd.to_datetime(timestamp, unit='ms').strftime('%Y-%m-%d %H:%M:%S')
    else:
        return None

def read_credentials_from_config(file_path):
    try:
        # Parse the config file
        config = configparser.ConfigParser()
        config.read(file_path)
        #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.get('bq', 'SERVICE_ACCNT_JSON_LOC')
        # Access the credentials
        creds = {
            "APP_NAME": config.get('Credentials', 'APP_NAME'),
            "APP_SOURCE": config.get('Credentials', 'APP_SOURCE'),
            "USER_ID": config.get('Credentials', 'USER_ID'),
            "PASSWORD": config.get('Credentials', 'PASSWORD'),
            "USER_KEY": config.get('Credentials', 'USER_KEY'),
            "ENCRYPTION_KEY": config.get('Credentials', 'ENCRYPTION_KEY'),
            "CLIENTCODE": config.get('Credentials', 'CLIENTCODE'),
            "USER_PIN": config.get('Credentials', 'USER_PIN'),
            "PROJECT_ID": config.get('bq', 'PROJECT_ID'),
            "DATASET_ID": config.get('bq', 'DATASET_ID'),
            "WALLET_BALANCE_TABLE_ID": config.get('bq', 'WALLET_BALANCE_TABLE_ID'),
            "HOLDINGS_TABLE_ID": config.get('bq', 'HOLDINGS_TABLE_ID'),
            "MARKET_DEPTH_TABLE_ID": config.get('bq','MARKET_DEPTH_TABLE_ID'),

        }

        return creds

    except configparser.Error as e:
        print(f"Error reading config file: {e}")
        return None

def perform_totp_login(email_id, totp, pin, user_key):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/TOTPLogin'
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "head": {
            "Key": user_key
        },
        "body": {
            "Email_ID": email_id,
            "TOTP": totp,
            "PIN": pin
        }
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            response_data = response.json()
            # Process the response data here
            request_token = response_data['body']['RequestToken']
            return request_token
        else:
            print(f"Request failed with status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        sys.exit(99)

def get_access_token(request_token, encry_key, user_id, user_key, file_path):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/GetAccessToken'
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        "head": {
            "Key": user_key
        },
        "body": {
            "RequestToken": request_token,
            "EncryKey": encry_key,
            "UserId": user_id
        }
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            response_data = response.json()
            # Process the response data here
            access_token = response_data['body']['AccessToken']
            with open(file_path, 'w') as file:
                file.write(access_token)
            return access_token
        else:
            print(f"Request failed with status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Request failed: {e}")

def get_market_status(access_token, user_key, client_code):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/MarketStatus'
    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json'
    }

    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:

            market_status = response.json()['body']['Data']
            print(market_status)
            market_status_df = pd.DataFrame(market_status)
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print(f"\t\t\t MARKET STATUS AS OF NOW {datetime.datetime.now()}")
            print(tabulate(market_status_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return True
        else:
            return None  # Handle the error here or return as per your needs

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_market_depth_request(access_token, user_key, client_code):
    file_path = 'script_codes.txt'  # Replace 'your_file.txt' with your file path
    data = []
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file, delimiter='|')
        for row in csv_reader:
            # Extracting data and converting ScripCode to integers
            entry = {
                "Exchange": row['Exch'].strip(),
                "ExchangeType": row['ExchType'].strip(),
                "ScripCode": int(row['Scripcode'].strip())
            }
            data.append(entry)
    json_data = {"Data": data}
    json_string = json.dumps(json_data, indent=4)
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/MarketDepth'

    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json',
        'Cookie': '5paisacookie=qwmwpam1su3s4lvwlwyevrl5; NSC_JOh0em50e1pajl5b5jvyafempnkehc3=ffffffffaf103e0f45525d5f4f58455e445a4a423660'
    }

    request_data = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code,
            "Count": "1",
            "Data" : data
            # "Data": [
            #     {
            #         "Exchange": "N",
            #         "ExchangeType": "C",
            #         "ScripCode": 3494
            #     },
            #     {
            #         "Exchange": "N",
            #         "ExchangeType": "C",
            #         "ScripCode": 14428
            #     }
            #     ,
            #     {
            #         "Exchange": "N",
            #         "ExchangeType": "C",
            #         "ScripCode": 18118
            #     }
            #     ,
            #     {
            #         "Exchange": "N",
            #         "ExchangeType": "C",
            #         "ScripCode": 4963
            #     }
            # ]
        }
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(request_data))
        response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)

        print("==========",json_data)

        if response.status_code == 200:
            market_depth = response.json()['body']['Data']
            # print(data)
            market_depth_df = pd.DataFrame(market_depth)
            market_depth_df['edw_publn_id'] = datetime.datetime.now()
            #market_depth_df['LastTradeTime'] = market_depth_df['LastTradeTime'].apply(convert_date)

            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print(f"\t\t\t MARKET STATUS/MARKET DEPTH AS OF NOW {datetime.datetime.now()}")
            print(tabulate(market_depth_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return market_depth_df
        else:
            return None


    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_holdings_request(access_token, user_key, client_code):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V3/Holding'
    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json',
        'Cookie': 'NSC_JOh0em50e1pajl5b5jvyafempnkehc3=ffffffffaf103e0f45525d5f4f58455e445a4a423660'
    }
    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            holding_details = response.json()['body']['Data']
            holding_details_df = pd.DataFrame(holding_details)
            holding_details_df['edw_publn_id'] = datetime.datetime.now()
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print("\t\t\t HOLDINGS DETAIL/HISTORY ORDERS FOR THE USER")
            print(tabulate(holding_details_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return holding_details_df
        else:
            print(f"Request failed with status code: {response.status_code}")
            return False  # API call failed
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        sys.exit(97)  # API call failed

def get_wallet_balance_request(access_token, user_key, client_code):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V3/Margin'
    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json',
        'Cookie': 'NSC_JOh0em50e1pajl5b5jvyafempnkehc3=ffffffffaf103e0f45525d5f4f58455e445a4a423660'
    }
    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            wallet_details = response.json()['body']['EquityMargin']
            # print(data)
            wallet_details_df = pd.DataFrame(wallet_details)
            wallet_details_df['edw_publn_id'] = datetime.datetime.now()
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print("\t\t\t WALLET BALANCE DETAILS THE USER")
            print(tabulate(wallet_details_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return wallet_details_df
        else:
            print(f"Request failed with status code: {response.status_code}")
            return False  # API call failed
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return False  # API call failed

def place_order(access_token, user_key, client_code, app_source,stock_code):
    url = 'https://openapi.5paisa.com/VendorsAPI/Service1.svc/V1/PlaceOrderRequest'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
        'Cookie': '5paisacookie=j2usr4wuddgegs14hcjrh3d4; NSC_JOh0em50e1pajl5b5jvyafempnkehc3=ffffffffaf103e0f45525d5f4f58455e445a4a423660'
    }
    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code,
            "Exchange": "N",
            "ExchangeType": "C",
            "Qty": "1",
            "Price": "0",
            "OrderType": "Buy",
            "ScripCode": stock_code,
            "IsIntraday": False,
            "DisQty": 1,
            "StopLossPrice": 0,
            "IsAHOrder": "N",
            "RemoteOrderID": "Automation",
            "AppSource": app_source
        }
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        # Convert the response text to a Python dictionary
        response_dict = json.loads(response.text)
        if response_dict['body']['RMSResponseCode'] == 0:
            print("Order placed successfully:", response.text)
        else:
            print("Failed to place order---> ERROR:", response_dict['body']['Message'])
        return response
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_order_status(access_token, user_key, client_code, exchange_type, remote_order_id):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V2/OrderStatus'

    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json'
    }

    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code,
            "OrdStatusReqList": [
                {
                    "Exch": exchange_type,
                    "RemoteOrderID": remote_order_id
                }
            ]
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an error for bad status codes

        if response.status_code == 200:
            order_status = response.json()['body']['OrdStatusResLst']
            order_status_df = pd.DataFrame(order_status)
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print("\t\t\t ORDER STATUS FOR THE USER's RemoteOrderID")
            print(tabulate(order_status_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return order_status_df
        else:
            return None

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_order_book(access_token, user_key, client_code):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V3/OrderBook'
    parsed_data = []

    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json',
        'Cookie': 'NSC_JOh0em50e1pajl5b5jvyafempnkehc3=ffffffffaf103e0f45525d5f4f58455e445a4a423660'
    }

    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code
        }
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an error for bad status codes

        if response.status_code == 200:
            orderbookdetails = response.json()['body']['OrderBookDetail']
            #print(data)
            orderbookdetails_df = pd.DataFrame(orderbookdetails)
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print("\t\t\t ORDERBOOK DETAIL/HISTORY ORDERS FOR THE USER")
            print(tabulate(orderbookdetails_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return orderbookdetails_df

        else:
            return None

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None

def get_trade_book_request(access_token, user_key, client_code):
    url = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/V1/TradeBook'

    headers = {
        'Authorization': f'bearer {access_token}',
        'Content-Type': 'application/json',
        'Cookie': 'NSC_JOh0em50e1pajl5b5jvyafempnkehc3=ffffffffaf103e0f45525d5f4f58455e445a4a423660'
    }

    payload = {
        "head": {
            "key": user_key
        },
        "body": {
            "ClientCode": client_code
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 200:
            data = response.json()['body']['TradeBookDetail']
            tradebook_df = pd.DataFrame(data)
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            print("\t\t\tTRADEBOOK/HOLDINGS FOR THE USER")
            print(tabulate(tradebook_df, headers='keys', tablefmt='fancy_grid'))
            print("\n\n\t\t\t++++++++++++++++++++++++++++++++++++++++++++")
            return tradebook_df
        else:
            return None  # Or handle the error here
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return None


def refresh_login_creds(success,credentials,totp):
    if not success:
        print("Calling totp Login Mechanism")
        request_token = perform_totp_login(email_id=credentials.get('CLIENTCODE'),
                                           totp=totp,
                                           pin=credentials.get('USER_PIN'),
                                           user_key=credentials.get('USER_KEY'))

        if request_token is not None:
            access_token = get_access_token(request_token=request_token,
                                            encry_key=credentials.get('ENCRYPTION_KEY'),
                                            user_id=credentials.get('USER_ID'),
                                            user_key=credentials.get('USER_KEY'),
                                            file_path='access_token.txt'
                                            )
        with open('access_token.txt', 'r') as file:
            access_token = file.read().strip()

if __name__ == '__main__':
    config_file_path = 'credienrtials_config.ini'

    # Read credentials from the config file
    credentials = read_credentials_from_config(config_file_path)
    project_id = credentials.get('PROJECT_ID')
    dataset_id = credentials.get('DATASET_ID')
    wallet_table_id = credentials.get('WALLET_BALANCE_TABLE_ID')
    holdings_table_id = credentials.get('HOLDINGS_TABLE_ID')
    market_depth_table_id = credentials.get('MARKET_DEPTH_TABLE_ID')
    # Read access token from file
    with open('access_token.txt', 'r') as file:
        access_token = file.read().strip()
    # Make API request using access token
    success = get_market_status(access_token=access_token, user_key=credentials.get('USER_KEY'),
                                   client_code=credentials.get('CLIENTCODE'))

    refresh_login_creds(success,credentials,'937208')

    market_depth_df = get_market_depth_request(access_token=access_token, user_key=credentials.get('USER_KEY'),
                                   client_code=credentials.get('CLIENTCODE'))
    market_depth_table_ref = f"{project_id}.{dataset_id}.{market_depth_table_id}"
    pandas_gbq.to_gbq(market_depth_df, market_depth_table_ref, project_id=project_id, if_exists='append')
    print("MARKET STATUS FOR BQ TABLE UPDATE SUCCESSFULLY COMPLETED!")

    # Construct the fully qualified table ID
    holdings_table_df = get_holdings_request(access_token=access_token, user_key=credentials.get('USER_KEY'),
                             client_code=credentials.get('CLIENTCODE'))
    holdings_table_ref = f"{project_id}.{dataset_id}.{holdings_table_id}"
    pandas_gbq.to_gbq(holdings_table_df, holdings_table_ref, project_id=project_id, if_exists='append')
    print("HOLDINGS FOR BQ TABLE UPDATE SUCCESSFULLY COMPLETED!")

    # Construct the fully qualified table ID
    table_ref = f"{project_id}.{dataset_id}.{wallet_table_id}"
    wallet_bal_df = get_wallet_balance_request(access_token=access_token, user_key=credentials.get('USER_KEY'),
                         client_code=credentials.get('CLIENTCODE'))
    pandas_gbq.to_gbq(wallet_bal_df, table_ref, project_id=project_id, if_exists='append')
    print("BQ TABLE UPDATE SUCCESSFULLY COMPLETED!")



    # place_order(access_token=access_token,
    #               user_key=credentials.get('USER_KEY'),
    #               client_code=credentials.get('CLIENTCODE'),
    #               app_source=credentials.get('APP_SOURCE'),
    #               stock_code="3494" #PGINVENT
    #               )

    # place_order(access_token=access_token,
    #             user_key=credentials.get('USER_KEY'),
    #             client_code=credentials.get('CLIENTCODE'),
    #             app_source=credentials.get('APP_SOURCE'),
    #             stock_code="14428"  # GOLDBEES
    #             )
    # place_order(access_token=access_token,
    #             user_key=credentials.get('USER_KEY'),
    #             client_code=credentials.get('CLIENTCODE'),
    #             app_source=credentials.get('APP_SOURCE'),
    #             stock_code="4963"  # ICICIBANK
    #             )

    get_order_status(access_token=access_token,
                     user_key=credentials.get('USER_KEY'),
                     client_code=credentials.get('CLIENTCODE'),
                     exchange_type='N',
                     remote_order_id='Automation'
                     )
    order_book = get_order_book(access_token=access_token,
                   user_key=credentials.get('USER_KEY'),
                   client_code=credentials.get('CLIENTCODE')
                   )

    # get_trade_book_request(access_token=access_token,
    #                user_key=credentials.get('USER_KEY'),
    #                client_code=credentials.get('CLIENTCODE')
    #                )
    #
    #----------------------------------NOT IN USE
    # if not success:
    #     print("Calling totp Login Mechanism")
    #     request_token = perform_totp_login(email_id=credentials.get('CLIENTCODE'),
    #                                        totp='310689',
    #                                        pin=credentials.get('USER_PIN'),
    #                                        user_key=credentials.get('USER_KEY'))
    #
    #     if request_token is not None:
    #         access_token = get_access_token(request_token=request_token,
    #                                         encry_key=credentials.get('ENCRYPTION_KEY'),
    #                                         user_id=credentials.get('USER_ID'),
    #                                         user_key=credentials.get('USER_KEY'),
    #                                         file_path='access_token.txt'
    #                                         )

