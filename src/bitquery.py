import pandas as pd
import argparse
import tools.config as config

parser = argparse.ArgumentParser(description='Do you want to backfill?')
parser.add_argument("--backfill", default='False', help="Backfill Data?")
args = parser.parse_args()

if __name__ == '__main__':
    frm_till = config.generate_dates(args.backfill)
    merged_df = pd.DataFrame()
    param_list = config.generate_params(frm_till['frm'], frm_till['till'])
    columns = ['exchangeName'
                , 'exchangeProtocol'
                , 'buySymbol'
                , 'buyAddress'
                , 'buyNumTokens'
                , 'sellSymbol'
                , 'sellAddress'
                , 'sellNumTokens'
                , 'numTrades'
                , 'tradeAmount'
                , 'date']
    for item in param_list:
        buy_query = config.generate_buy_query(item)
        buy_data_raw = config.run_query(buy_query)
        buy_df = pd.DataFrame()
        sell_df = pd.DataFrame()
        if len(buy_data_raw['data']['ethereum']['dexTrades']) > 0:
            for buy_data in buy_data_raw['data']['ethereum']['dexTrades']:
                buy_data_df = pd.DataFrame(columns=columns, data=[
                    [buy_data['exchange']['fullName'], buy_data['protocol'], buy_data['buyCurrency']['symbol'],
                     buy_data['buyCurrency']['address'], buy_data['buyAmount'], buy_data['sellCurrency']['symbol'],
                     buy_data['sellCurrency']['address'], buy_data['sellAmount'], buy_data['trades'],
                     buy_data['tradeAmount'], buy_data['date']['date']]])
                buy_df = pd.concat([buy_df, buy_data_df])
        sell_query = config.generate_sell_query(item)
        sell_query_raw = config.run_query(sell_query)
        if len(sell_query_raw['data']['ethereum']['dexTrades']) > 0:
            for sell_data in sell_query_raw['data']['ethereum']['dexTrades']:
                sell_data_df = pd.DataFrame(columns=columns, data=[
                    [sell_data['exchange']['fullName'], sell_data['protocol'], sell_data['buyCurrency']['symbol'],
                     sell_data['buyCurrency']['address'], sell_data['buyAmount'], sell_data['sellCurrency']['symbol'],
                     sell_data['sellCurrency']['address'], sell_data['sellAmount'], sell_data['trades'], sell_data['tradeAmount'],
                     sell_data['date']['date']]])
                sell_df = pd.concat([sell_df, sell_data_df])
        merged_df = pd.concat([merged_df, sell_df, buy_df])

    config.write_data(merged_df, args.backfill)