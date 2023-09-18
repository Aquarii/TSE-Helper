from pathlib import Path
import tomli, tomli_w
from datetime import datetime

root = Path(__file__).parent.parent
config_path = (Path(__file__).parent / 'config.toml').relative_to(root)
config_path.touch(exist_ok=True)
db_path = (Path(__file__).parent.parent / 'database').relative_to(root)
tickers_data_path = (Path(__file__).parent.parent / 'database' / 'tickers_data').relative_to(root)

# make config private later and def its getter
# def load():
with config_path.open(mode='rb') as file:
    item = tomli.load(file)
# return tomli.load(file)
# maybe later: getting item off of cfg= cfg.get(key1, key2,...)

def save(conf:dict):
    with config_path.open(mode='wb') as file:
        tomli_w.dump(conf, file)

#! make a config.toml checker function: checks if all last_updates aren't more than _last_date

default_items = {
    'LAST_UPDATE': {
        'INSTRUMENTS': 0, 
        'IDENTITY': 0,
        # ---------------------^-------------------
        'CAPITAL_INCREASE': 0,
        'INSTRUMENT_TYPES': datetime(2000, 1, 1).date()
    },
    'PATH': {
        'CONFIG_FILE': str(config_path),
        'DATABASE': str(db_path),
        'TICKERS_DATA': str(tickers_data_path)
    },
    'URI': {
        "IDENTITY" : "http://cdn.tsetmc.com/api/Instrument/GetInstrumentIdentity/{}",
        # -------------------^---------------------
        'DAILY_PRICES_HISTROY_TO_DATE':'http://service.tsetmc.com/tsev2/data/TseClient2.aspx?t=ClosingPrices&a={}',
        'DAILY_PRICES_LAST_N_DAYS':'http://www.old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={}&Top={}&A={}',
        'TRADE_DETAILS_HISTORY':'http://cdn.tsetmc.com/api/Trade/GetTradeHistory/{}/{}/{}',
        'TRADE_DETAILS_CURRENT_DAY':'http://www.old.tsetmc.com/tsev2/data/TradeDetail.aspx?i={}',
        'CLIENT_TYPE_HISTORY':'http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{}/{}',
        'SHAREHOLDER_HISTORY':'http://cdn.tsetmc.com/api/Shareholder/{}/{}',
        'IDENTITY':'http://cdn.tsetmc.com/api/Instrument/GetInstrumentIdentity/{}'
    },
    # 'PG_DB':{
    #     'HOST': '127.0.0.1',
    #     'NAME': '',
    #     'USERNAME': '',
    #     'PASSWORD': '',
        # 'HEALTH': 'STATUS@DATETIME' # later
    # }
}

if not item: 
    item = default_items
    save(item)
