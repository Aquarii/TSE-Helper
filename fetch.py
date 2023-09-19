from typing import Iterable, TypedDict
from io import StringIO
from glob import glob
import os.path, time
from os import path
import zeep
from tenacity import retry, retry_if_exception_type, wait_random, stop_after_attempt
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

class PriceQuotes(TypedDict):
    insCode: str
    quotes: pd.DataFrame

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


def api_instruments(last_fetch=0):
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
            "dEven",
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
            "dEven",
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
_recent_instruments_df = api_instruments(0)


################################################################################################
#                                           INSTRUMENTS                                        #
################################################################################################

def get_catalogue():
    
    instruments = _recent_instruments_df
    instruments.to_csv(f'{config.db_path}/instruments.csv', quoting=1) # Dev-Only. Original Data 
    
    identities = get_identities()
    updates = get_last_update_dates()
    
    catalogue = instruments.merge(
        identities[[
            'lSecVal',
            'lSoSecVal',
            'cgrValCotTitle'
        ]], 
        how='outer', 
        on='insCode'
    )
    catalogue = catalogue.merge(
        updates['daily'], 
        how='outer', 
        on='insCode'
    )
    
    catalogue.to_csv(f'{config.db_path}/catalogue.csv', quoting=1)
    
    return catalogue


def get_last_update_dates():
    
    last_updates_csv_path = str(config.db_path)+'/last_updates.csv'
    
    if not path.isfile(last_updates_csv_path):
        
        last_updates_df = pd.DataFrame()
        last_updates_df = _recent_instruments_df[['dEven']]
        last_updates_df['daily'] = 0
        last_updates_df.to_csv(last_updates_csv_path, index=True, quoting=1) # Dev-Only. Original Data 
        
    else:
        
        last_updates_df = pd.read_csv(
            last_updates_csv_path, 
            index_col='insCode', 
            dtype={
                'insCode':str, 
                'dEven':str,
                'daily':str
            }
        )
        
        if set(_recent_instruments_df.index) > set(last_updates_df.index):
            
            last_updates_df = _recent_instruments_df[['dEven']].merge(
                last_updates_df['daily'], 
                how= 'outer', 
                on='insCode'
            )
            last_updates_df.to_csv(last_updates_csv_path, index=True, quoting=1) # Dev-Only. Original Data 
        
    return last_updates_df


################################################################################################
#                                           IDENTITY                                           #
################################################################################################

# @retry(
#     retry=retry_if_exception_type(requests.exceptions.HTTPError),
#     wait=wait_random(min=1, max=4),
#     stop=stop_after_attempt(7)
# )
def get_identity(insCode:str) -> dict:
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info(f'Going for Identity of "{insCode}"')
    #endregion#
    
    result = requests.get(
        config.item['URI']['IDENTITY'].format(insCode), headers=request_headers, cookies=cookie_jar
    )
    result = json.loads(result.text)
    
    #region#  <<<<<< LOG >>>>>>
    # data_log.info(get_identity.retry.statistics)
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
    ).drop(
        [
            'sector_dEven', 
            'subSector_dEven', 
            'subSector_cSecVal', 
            'insCode',
            'flow'
        ], 
        axis=0
    ).transpose()
    identities.rename(
        columns= {
            col_name:col_name.split('_')[-1] 
            for col_name in identities.columns 
            if '_' in col_name
        }, 
        inplace= True
    )
    identities['cSecVal'] = identities['cSecVal'].str.strip()
    identities.index.name = 'insCode'
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################### Initializing Identities Finished ###################')
    #endregion#
    
    # Save to CSV # Dev-Only. Original Data 
    identities.to_csv(str(config.db_path) + '/identities.csv', quoting=1) #make it 5 after update to python 3.12 
    
    return identities


def get_identities() -> pd.DataFrame:
    
    identities_csv_path = str(config.db_path)+'/identities.csv'
    
    if path.isfile(identities_csv_path):
        identities = pd.read_csv(
            identities_csv_path, 
            index_col='insCode', 
            dtype={
                'insCode': str, 
                'cSecVal': str,
                'cSoSecVal': str,
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
            ).drop(
                [
                    'sector_dEven', 
                    'subSector_dEven', 
                    'subSector_cSecVal', 
                    'insCode'
                ], 
                axis=0
            ).transpose()
            new_identities.rename(
                columns= {
                    col_name:col_name.split('_')[-1] 
                    for col_name in new_identities.columns 
                    if '_' in col_name
                }, 
                inplace= True
            )
            new_identities['cSecVal'] = new_identities['cSecVal'].str.strip()
            new_identities.index.name = 'insCode'
            
            identities = pd.concat([identities, new_identities])
            
            # save to csv # Dev-Only. Original Data
            identities.to_csv(str(config.db_path) + '/identities.csv', quoting=1) #make it 5 after update to python 3.12 
        
        else:
            #region#  <<<<<< LOG >>>>>>
            data_log.info('No New (Non-Index) Instruements.')
            #endregion#
        
        # config.item['LAST_UPDATE']['IDENTITY'] = _last_date # Obsolete! Effective date is last_update_instruments. will keep it for now.
        # config.save(config.item)
        
        return identities
    
    else:
        print('"identities.csv" Not Found. Downloading... ~3m-20m (Night/Rush Hour)')
        return init_identities()


#################################################################################################
##                                INSTRUMENT PRICES OHLCV + CSV                                ##
#################################################################################################

def get_closing_prices_daily(
    insCodes: str|Iterable[str], 
    traded_instruments_only = True,
    force_download= False
) -> PriceQuotes:
    #region# <<<<<< DOCSTRING >>>>>>
    """Gets the latest update of given instruments. \
        downloads closing price quotes if database is out-of-date, \
        otherwise reads from database.

    Args:
        insCodes (str | Iterable[str]): Instruments' insCode(s)
        force_download (bool, optional): Ignore and re-write database. Defaults to False.

    Returns:
        PriceQuotes: Prices in format: dict[key:str, value:pd.DataFrame]
    """
    #endregion#
    
    if isinstance(insCodes,str):
        insCodes = [insCodes] 
    
    if insCodes == ['all']:
        insCodes = _recent_instruments_df.index
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################## Download Started: Daily Prices ##################')
    #endregion#
    
    for insCode in insCodes:
        
        last_updates_df = get_last_update_dates()
        
        # skip instrument if it's up-to-date or download not forced
        if (
            not force_download and
            path.isfile(f'{config.tickers_data_path}/{insCode}.csv') and
            last_updates_df.at[insCode,'daily'] == _last_date
        ):
            continue
        
        # update instrument if it's new or out-of-date or forced
        else:
            #TODO: catch error 500 and print appropriate notice
            #region#  <<<<<< LOG >>>>>>
            data_log.info(f'Take a Shot at Daily Quotes of: {insCode}')
            #endregion#
            
            resp = requests.get(
                url=f"http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyListCSV/{insCode}/{insCode}",
                headers=request_headers,
                cookies=cookie_jar)
            resp.raise_for_status()
            
            with open(f'{config.tickers_data_path}/{insCode}.csv', 'wb') as file:
                file.write(resp.content)
            
            last_updates_df.at[insCode,'daily'] = _last_date
            last_updates_df.to_csv(f'{config.db_path}/last_updates.csv', index=True, quoting=1)
            
            #region#  <<<<<< LOG >>>>>>
            data_log.info(f'Daily Prices of {insCode} Downloaded and Saved as CSV.')
            #endregion#
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################## Download Successeded: Daily Prices ##################')
    #endregion#
    
    quotes = load_quotes_csv(insCodes)
    
    return {
        key: quotes[key] 
        for key in quotes.keys() 
        if quotes[key].index[-1] == _last_date
    } if traded_instruments_only else quotes


@calculate_MA
@clean_data(remove_days_with_no_trades=True)
def load_quotes_csv(insCodes: list) -> PriceQuotes: 
    try:        
        daily_prices = {insCode: pd.read_csv(
            f'{config.tickers_data_path}/{insCode}.csv',
            dtype={
                "<TICKER>": str,
                "<DTYYYYMMDD>": str,
                "<FIRST>": "float64",
                "<HIGH>": "float64",
                "<LOW>": "float64",
                "<CLOSE>": "float64",
                "<VALUE>": "float64",
                "<VOL>": "int64",
                "<OPENINT>": "int64",
                "<PER>": str,
                "<OPEN>": "float64",
                "<LAST>": "float64"
            }
        ).sort_index(ascending=True) for insCode in insCodes} # sort just in case 
    
    except (Exception) as e:
            print(f"Error: {e}")
    
    #region#  <<<<<< LOG >>>>>>
    data_log.info('################## Daily Prices Loaded from Database ##################')
    #endregion#
    
    return daily_prices

