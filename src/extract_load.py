# Import
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Import das variáveis de ambiente
load_dotenv()
engine_string = os.getenv('DB_STRING_PROD')
commodities = ['CL=F', 'GC=F', 'SI=F']


def search_commodities_data(symbol, period='5d', interval='1d'):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period=period, interval=interval)[['Close']]
    data['symbol'] = symbol

    return data


def search_all_commodities_data(commodities):
    all_data = []
    for symbol in commodities:
        data = search_commodities_data(symbol)
        all_data.append(data)
    
    return pd.concat(all_data)


def save_on_database(df, schema='public'):
    engine = create_engine(engine_string)
    df.to_sql('commodities', con=engine, schema=schema, if_exists='append', index_label='date', index=True)

if __name__ == "__main__":
    concat_data = search_all_commodities_data(commodities)
    save_on_database(concat_data)

# Cotação dos meus ativos


# Concatenar os ativos (1..2..3) -> (1)


# Salvar no banco de dados