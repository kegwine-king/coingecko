import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import argparse
import requests

api_key = 'BQYndxUo4KnCR1bs69HAc9cHu7BDau5M'
url = 'https://graphql.bitquery.io/'

date_format = "%Y-%m-%d"


def generate_dates(backfill):
    if backfill == 'True':
        till = datetime.today().strftime('%Y-%m-%d')
        frm = (datetime.today() - relativedelta(months=12)).strftime('%Y-%m-%d')
    else:
        till = datetime.today().strftime('%Y-%m-%d')
        frm = datetime.today().strftime('%Y-%m-%d')
    dates = {"till":till, 'frm':frm}
    return dates


def generate_params(frm, till):
    param_list = []
    query_df = pd.read_csv('../data/query_data.csv')
    for i, x in query_df.iterrows():
        params = {
            "token_address": x['token_address'],
            "exchange_name": x['exchange_name'],
            "protocol_name": x['protocol_name'],
            "frm": frm,
            "till": till,
            "date_format": date_format,
        }
        param_list.append(params)
    return param_list


def generate_buy_query(params):
    query = """query{{
      ethereum(network: ethereum) {{
        dexTrades(
          options: {{desc: \"date.date\"}}
          exchangeName: {{is: \"{exchange_name}\"}}
          protocol: {{is: \"{protocol_name}\"}}
          date: {{since: \"{frm}\", till:\"{till}\"}}
          buyCurrency: {{is: \"{token_address}\"}}
          sellCurrency:{{}}
        ) {{
          buyCurrency{{
            symbol
            address
          }}
          sellCurrency{{
            symbol
            address
          }}
            trades: count
            tradeAmount(in: USD)
            date {{
                date(format:\"{date_format}\")
          }}
            exchange{{
                fullName
                address {{
                    address
                }}
            }}
            protocol
            buyAmount(calculate: sum)
            sellAmount(calculate: sum)
        }}
      }}
    }}""".format(frm=params['frm'], exchange_name=params['exchange_name'], protocol_name= params['protocol_name'], till=params['till'], date_format=params['date_format'], token_address=params['token_address'])

    return query

def generate_sell_query(params):
    query = """query{{
      ethereum(network: ethereum) {{
        dexTrades(
          options: {{desc: \"date.date\"}}
          exchangeName: {{is: \"{exchange_name}\"}}
          protocol: {{is: \"{protocol_name}\"}}
          date: {{since: \"{frm}\", till:\"{till}\"}}
          buyCurrency: {{}}
          sellCurrency:{{is: \"{token_address}\"}}
        ) {{
          buyCurrency{{
            symbol
            address
          }}
          sellCurrency{{
            symbol
            address
          }}
            trades: count
            tradeAmount(in: USD)
            date {{
                date(format:\"{date_format}\")
          }}
            exchange{{
                fullName
                address {{
                    address
                }}
            }}
            protocol
            buyAmount(calculate: sum)
            sellAmount(calculate: sum)
        }}
      }}
    }}""".format(frm=params['frm'], exchange_name=params['exchange_name'], protocol_name= params['protocol_name'], till=params['till'], date_format=params['date_format'], token_address=params['token_address'])

    return query


def run_query(query):  # A simple function to use requests.post to make the API call.
    headers = {'X-API-KEY': api_key}
    request = requests.post('https://graphql.bitquery.io/',
                            json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed and return code is {}.      {}'.format(request.status_code,
                        query))


def write_data(df, backfill):
    if backfill == 'True':
        df.to_csv('../data/output/DEX_data.csv', index= False)
    else:  # else append
        df.to_csv('../data/output/DEX_data.csv', mode='a', index=False, header=False)

