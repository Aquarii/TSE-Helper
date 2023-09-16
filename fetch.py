from typing import Union
from io import StringIO
from glob import glob
import os.path, time
from os import path
import zeep
from utils import (
    debug_log,
    data_log,
    ar_to_fa_series,
    ar_to_fa,
    fa_to_ar,
    fa_to_ar_series,
    flatten_json,
)
import config
from decorators import calculate_MA, clean_data
import requests
import json
import pandas as pd


request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Referer": "http://main.tsetmc.com/StaticContent/WebServiceHelp",
}
cookie_jar = {"ASP.NET_SessionId": "wa40en1alwxzjnqehjntrv5j"}

################################################################################################
#                                      TseClient 2.0 API                                       #
################################################################################################

def api_last_possible_deven():
    client = zeep.Client("http://service.tsetmc.com/WebService/TseClient.asmx?wsdl")
    last_workday = client.service.LastPossibleDeven()
    return (
        last_workday.split(";")[0]
        if last_workday.split(";")[1] == last_workday.split(";")[0]
        else print(last_workday)
    )  # for debuging purposes


def api_instruments(last_fetch):
    '''
    last_fetch:\n\tDate after which Traded Instruments are needed.
    '''
    client = zeep.Client(
        wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl'
    )
    
    instruments = client.service.Instrument(last_fetch)
    if instruments:
        columns = [
            "insCode",
            "instrumentID",
            "cValMne",
            "lVal18",
            "cSocCSAC",
            "lVal18AFC",
            "lVal30",
            "cIsin",
            "lastDate",
            "flow",
            "lSoc30",
            "CGdSVal",
            "cgrValCot",
            "yMarNSC",
            "cComVal",
            "cSecVal",
            "cSoSecVal",
            "yVal",
        ]
        instruments = pd.DataFrame(
            [instrument.split(',') for instrument in instruments.split(';')],# checkpoint,
            columns=columns
        )
        instruments.set_index("insCode", inplace=True)
        instruments["lVal18AFC"] = ar_to_fa_series(instruments["lVal18AFC"])
        instruments["lVal30"] = ar_to_fa_series(instruments["lVal30"])
        instruments["lSoc30"] = ar_to_fa_series(instruments["lSoc30"])
        instruments["cSecVal"] = instruments["cSecVal"].str.strip()
        return instruments
    else:
        print('No Response from Endpoint: Instrument')


def api_instrument_and_share(last_fetch_date=0, last_record_id=0):
    client = zeep.Client(
        wsdl="http://service.tsetmc.com/WebService/TseClient.asmx?wsdl"
    )
    
    instruments, share_increase = client.service.InstrumentAndShare(
        last_fetch_date, last_record_id
    ).split("@")
    
    if instruments:
        columns = [
            "insCode",
            "instrumentID",
            "cValMne",
            "lVal18",
            "cSocCSAC",
            "lVal18AFC",
            "lVal30",
            "cIsin",
            "lastDate",
            "flow",
            "lSoc30",
            "CGdSVal",
            "cgrValCot",
            "yMarNSC",
            "cComVal",
            "cSecVal",
            "cSoSecVal",
            "yVal",
        ]
        instruments = pd.DataFrame(
            [instrument.split(",") for instrument in instruments.split(";")],
            columns=columns
        )
        instruments.set_index("insCode", inplace=True)
        instruments["lVal18AFC"] = ar_to_fa_series(instruments["lVal18AFC"])
        instruments["lVal30"] = ar_to_fa_series(instruments["lVal30"])
        instruments["lSoc30"] = ar_to_fa_series(instruments["lSoc30"])
        instruments["cSecVal"] = instruments["cSecVal"].str.strip()
        
    if share_increase:
        share_increase = share_increase.split(";")
        share_increase = pd.DataFrame([share.split(",") for share in share_increase])
        share_increase.columns = [
            "record_id",
            "insCode",
            "date",
            "before_raise",
            "after_raise",
        ]
        # capital_increase['record_id'] = capital_increase['record_id'].astype(int)
        share_increase.set_index("record_id", inplace=True)
    else:
        share_increase = None
        
    return instruments, share_increase


_last_date = api_last_possible_deven()
_recent_instruments_df, _ = api_instrument_and_share(0, 0)


################################################################################################
#                                           INSTRUMENTS                                        #
################################################################################################

def get_instruments():
    
    instruments_csv_path = str(config.db_path)+'/instruments.csv'
    
    if config.item['LAST_UPDATE']['INSTRUMENTS'] == _last_date and path.isfile(instruments_csv_path):
        instruments = pd.read_csv(
            instruments_csv_path, index_col='insCode', 
            dtype={
                'insCode':str, 
                'cComVal':str
                }
        )
        
    else:
        instruments = _recent_instruments_df
        instruments.to_csv(f'{instruments_csv_path}/instruments.csv')
        
        config.item['LAST_UPDATE']['INSTRUMENTS'] = _last_date
        
    config.save(config.item)
    
    return instruments


################################################################################################
#                                           IDENTITY                                           #
################################################################################################

def get_identity(insCode:str) -> dict:
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info(f'Going for Identity of "{insCode}"')
    #endregion#
    
    result = requests.get(
        config.item['URI']['IDENTITY'].format(insCode), headers=request_headers, cookies=cookie_jar
    )
    result.raise_for_status()  # raises exception when not a 2xx response
    result = json.loads(result.text)
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info(f'Identity of "{insCode}" recieved correctly.')
    #endregion#
    
    return flatten_json(result["instrumentIdentity"])


def init_identities() -> pd.DataFrame:
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################### Initializing Identities Started ###################')
    #endregion#
    
    all_tickers_except_indices = _recent_instruments_df[_recent_instruments_df['cComVal'] != '6'].index
    identities = pd.DataFrame(
        {insCode:get_identity(insCode) for insCode in all_tickers_except_indices}
    ).transpose()
    identities['sector_cSecVal'] = identities['sector_cSecVal'].str.strip()
    identities.index.name = 'insCode'
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################### Initializing Identities Finished ###################')
    #endregion#
    
    # Save to CSV
    identities.to_csv(str(config.db_path) + '/identities.csv', quoting=1)
    
    return identities


def get_identities() -> pd.DataFrame:
    
    identities_csv_path = str(config.db_path)+'/identities.csv'
    
    if path.isfile(identities_csv_path):
        identities = pd.read_csv(
            identities_csv_path, 
            index_col='insCode', 
            dtype={
                'insCode': str, 
                'sector_cSecVal': str,
                'subSector_cSoSecVal': str,
                'yVal': str,
                'flow': str,
                'cComVal': str,
                }
            )
        
        all_tickers_except_indices = _recent_instruments_df[_recent_instruments_df['cComVal'] != '6'].index
        new_instruments = set(all_tickers_except_indices) - set(identities.index)
        
        if new_instruments:
            #region#  <<<<<< LOG >>>>>>
            data_log.info('New Instruments added. Getting Identity...')
            #endregion#
            
            new_identities = pd.DataFrame(
                {insCode:get_identity(insCode) for insCode in new_instruments}
            ).transpose()
            new_identities['sector_cSecVal'] = new_identities['sector_cSecVal'].str.strip()
            new_identities.index.name = 'insCode'
            
            identities = pd.concat([identities, new_identities])
            
            # save to csv
            identities.to_csv(str(config.db_path) + '/identities.csv', quoting=1)
        
        else:
            #region#  <<<<<< LOG >>>>>>
            data_log.info('No New (Non-Index) Instruements.')
            #endregion#
        
        config.item['LAST_UPDATE']['IDENTITY'] = _last_date # Obsolete! Effective date is last_update_instruments. will keep it for now.
        config.save(config.item)
        
        return identities
    
    else:
        print('"identities.csv" file doesn\'t exists. re-downloading its data... aprox. 20 mins.')
        return init_identities()


#################################################################################################
##                                INSTRUMENT PRICES OHLCV + CSV                                ##
#################################################################################################

def get_quotes(insCodes:Union[str,list], force_download=False): #? timeframe param "all"|"daily"|"2m"|"30m"
    
    if isinstance(insCodes,str):
        insCodes = [insCodes] 
    
    if config.item['LAST_UPDATE']['DAILY_PRICES'] < api_last_possible_deven():
        print('Database is Outdated. Updating... Please Wait...')
        force_download = True
    
    if force_download:
        #region#  <<<<<< LOG >>>>>>
        data_log.info('################## Daily Prices: Download Started ##################')
        #endregion#
        
        download_daily_timeframe_to_csv()
        
        config.item['LAST_UPDATE']['DAILY_PRICES'] = _last_date
        config.save(config.item)
        
        #region#  <<<<<< LOG >>>>>>
        data_log.info('################## Daily Prices: Download Successeded ##################')
        #endregion#
    
    return load_prices_csv(insCodes)


def download_daily_timeframe_to_csv():
    
    for insCode in _recent_instruments_df.index:
        
        csv_file = f'./database/tickers_data/{insCode}.csv'
        instrument_last_update_date = time.strftime('%Y%m%d', time.localtime(os.path.getmtime(csv_file)))
        
        # skip instrument if it's up-to-date
        if path.isfile(csv_file) and instrument_last_update_date == _last_date:
            continue
        
        # update instrument if it's new or out-of-date
        else:
            #region#  <<<<<< LOG >>>>>>
            data_log.info(f'Take a Shot at insCode: {insCode}')
            #endregion#
            
            resp = requests.get(
                url=f"http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyListCSV/{insCode}/{insCode}",
                headers=request_headers,
                cookies=cookie_jar)
            resp.raise_for_status()
            
            data = StringIO(resp.text)
            df = pd.read_csv(data)
            df.to_csv(f'{config.tickers_data_path}/{insCode}.csv')
            
            #region#  <<<<<< LOG >>>>>>
            data_log.info(f'Daily Prices of {insCode} Downloaded and Saved as CSV.')
            #endregion#


################################################################################################
#                                     LOAD PRICES FROM CSV                                     #
################################################################################################

@calculate_MA
@clean_data(remove_days_with_no_trades=True)
def load_prices_csv(insCodes: list) -> dict: 
    try:        
        daily_prices = {insCode: pd.read_csv(
            f'{config.tickers_data_path}/{insCode}.csv',
            dtype={
                "<LOW>": "float32",
                "<HIGH>": "float32",
                "<OPEN>": "float32",
                # "<VALUE>": "uint64",
            }
        ) for insCode in insCodes}
    
    except (Exception) as e:
            print(f"Error: {e}")
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################## Daily Prices Loaded from Database ##################')
    #endregion#
    
    return daily_prices

