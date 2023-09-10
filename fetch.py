from typing import Union
from io import StringIO
from glob import glob
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
#                                       LAST TRADING DAY                                       #
################################################################################################

def last_possible_deven():
    client = zeep.Client("http://service.tsetmc.com/WebService/TseClient.asmx?wsdl")
    last_workday = client.service.LastPossibleDeven()
    return (
        last_workday.split(";")[0]
        if last_workday.split(";")[1] == last_workday.split(";")[0]
        else print(last_workday)
    )  # for debuging purposes

################################################################################################
#                                       GET INSTRUMENTS                                        #
################################################################################################

def instruments(last_fetch):
    '''
    last_fetch:\n\tDate after which Traded Instruments are needed.
    '''
    client = zeep.Client(
        wsdl='http://service.tsetmc.com/WebService/TseClient.asmx?wsdl'
    )
    
    instruments = client.service.Instrument(last_fetch)
    if instruments:
        instruments = pd.DataFrame(
            instrument.split(',') for instrument in instruments.split(';'))
        instruments.columns = [
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
            "status_code",
            "group_code",
            "market_type_code",
            "cComVal",
            "cSecVal",
            "cSoSecVal",
            "yVal",
        ]
        instruments.set_index("insCode", inplace=True)
        instruments["lVal18AFC"] = ar_to_fa_series(instruments["lVal18AFC"])
        instruments["lVal30"] = ar_to_fa_series(instruments["lVal30"])
        instruments["lSoc30"] = ar_to_fa_series(instruments["lSoc30"])
        instruments["cSecVal"] = instruments["cSecVal"].str.strip()
        return instruments
    else:
        print('No Response from Endpoint: Instrument')


################################################################################################
#                                  GET INSTRUMENTS AND SHARES                                  #
################################################################################################

def InstrumentAndShare(last_fetch_date=0, last_record_id=0):
    client = zeep.Client(
        wsdl="http://service.tsetmc.com/WebService/TseClient.asmx?wsdl"
    )

    instruments, share_increase = client.service.InstrumentAndShare(
        last_fetch_date, last_record_id
    ).split("@")

    if instruments:
        instruments = instruments.split(";")
        instruments = pd.DataFrame(
            [instrument.split(",") for instrument in instruments]
        )
        instruments.columns = [
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
            "status_code",
            "group_code",
            "market_type_code",
            "cComVal",
            "cSecVal",
            "cSoSecVal",
            "yVal",
        ]
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


last_date = last_possible_deven()
recent_instruments, _ = InstrumentAndShare(0, 0)


################################################################################################
#                                           IDENTITY                                           #
################################################################################################

def get_identity(insCode):
    
        data_log.info(f'Going for Identity of "{insCode}"')
        
        result = requests.get(
            config.item['URI']['IDENTITY'].format(insCode), headers=request_headers, cookies=cookie_jar
        )
        result.raise_for_status()  # raises exception when not a 2xx response
        result = json.loads(result.text)
        
        data_log.info(f'Identity of "{insCode}" recieved correctly.')
        
        return flatten_json(result["instrumentIdentity"])


def init_identities():
    
    all_tickers_except_indices = recent_instruments[recent_instruments['cComVal'] != '6'].index
    
    data_log.info('################### Initializing Identities Started ###################')
    
    identities = pd.DataFrame({index:get_identity(index) for index in all_tickers_except_indices}).transpose()
    identities.index.name = 'insCode'
    
    data_log.info('################### Initializing Identities Finished ###################')
    
    # save to csv
    identities.to_csv(str(config.db_path) + '/identities.csv')
    
    return identities


def update_identities():
    # اینبار تو یک دق و ۲۵ ثانیه تموم شد! و اینکه تا قبل از این ساعت یعنی ۸ و ۴۹ شب ارور میداد ک ریسپانس خالی میگرفت.
    # شاید از ی ساعتی ببعد ترافیک سرور کم میشه و راحت میشه دیتا گرفت. بعدا چک کنم ک مطمئن بشم
    # Update: without proxy (even with bypassing tsetmc.com) works better.
    
    identities_csv_file = str(config.db_path)+'/identities.csv'
    
    if path.isfile(identities_csv_file):
        
        all_tickers_except_indices = recent_instruments[recent_instruments['cComVal'] != '6'].index
        
        identities = pd.read_csv(identities_csv_file, index_col='insCode', dtype={'insCode':str})
        new_instruments = set(all_tickers_except_indices) - set(identities.index)
        
        if new_instruments:
            
            data_log.info('New Instruments added. Getting Identity...')
            
            new_identities = pd.DataFrame({index:get_identity(index) for index in new_instruments}).transpose()
            identities = pd.concat([identities, new_identities])
            
            # save to csv
            identities.to_csv(str(config.db_path) + '/identities.csv')
        
        else:
            data_log.info('No new (Non-Index) Instruements added.')
    else:
        print('"identities.csv" file doesn\'t exists. re-downloading its data... aprox. 20 mins.')
        
        identities = init_identities()
    
    config.item['LAST_UPDATE']['IDENTITY'] = last_date
    config.save(config.item)
    
    return identities


def identities():
    '''Wrapper for update_identities()'''
    
    return update_identities() \
        if config.item['LAST_UPDATE']['IDENTITY'] <= last_date \
            else print('Faulty Config!')


################################################################################################
#                                 INSTRUMENT DAILY OHLCV + CSV                                 #
################################################################################################

@calculate_MA
@clean_data
def get_daily_prices(insCodes:Union[str,list], force_download=False):
    
    if isinstance(insCodes,str):
        insCodes = [insCodes] 
    
    if config.item['LAST_UPDATE']['DAILY_PRICES'] < last_possible_deven():
        force_download = True
    
    if force_download:
        
        daily_prices = {}
        
        data_log.info('################## Downloading Daily Prices Started ##################')
        
        for insCode in insCodes:
            resp = requests.get(
                url=f"http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyListCSV/{insCode}/{insCode}",
                headers=request_headers,
                cookies=cookie_jar)
            resp.raise_for_status()
            
            data = StringIO(resp.text)
            df = pd.read_csv(data)
            df = df.set_index('<TICKER>')
            
            df.to_csv(f'{config.tickers_data_path}/{insCode}.csv')
            
            data_log.info(f'Downloaded Daily Prices for: {insCode}')
            
            daily_prices[insCode] = df
            
            data_log.info(f'Daily Prices CSV Saved for: {insCode}')
        
        config.item['LAST_UPDATE']['DAILY_PRICES'] = last_date
        config.save(config.item)
        
        data_log.info('################## Downloading Daily Prices Successeded ##################')
    
    else:
        daily_prices = load_prices_csv(insCodes=insCodes)

    return daily_prices


################################################################################################
#                                     LOAD PRICES FROM CSV                                     #
################################################################################################

# @calculate_MA(active=True)
def load_prices_csv(insCodes: list) -> dict: 
    try:        
        daily_prices = {insCode: pd.read_csv(
                config.db_path + "/" + insCode + ".csv",
                dtype={
                    "<LOW>": "float32",
                    "<HIGH>": "float32",
                    "<OPEN>": "float32",
                    # "<VALUE>": "uint64",
                },
                index_col='<TICKER>'
            ) for insCode in insCodes}
    
    except (Exception) as e:
            print(f"Error: {e}")
    
    data_log.info('################## Daily Prices Loaded from Database ##################')
    
    return daily_prices
