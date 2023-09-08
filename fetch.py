from typing import Union
from io import StringIO
from glob import glob
from os.path import normpath
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
from decorators import calculate_MA
import requests
import json
import pandas as pd


request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
    "Referer": "http://main.tsetmc.com/StaticContent/WebServiceHelp",
}
cookie_jar = {"ASP.NET_SessionId": "wa40en1alwxzjnqehjntrv5j"}


def last_possible_deven():
    client = zeep.Client("http://service.tsetmc.com/WebService/TseClient.asmx?wsdl")
    last_workday = client.service.LastPossibleDeven()
    return (
        last_workday.split(";")[0]
        if last_workday.split(";")[1] == last_workday.split(";")[0]
        else print(last_workday)
    )  # for debuging purposes

################################################################################################
#                                         GET IDENTITY                                         #
################################################################################################

def get_identity(insCode):
    data_log.info(f'Going for Identity of "{insCode}"')
    result = requests.get(
        config.IDENTITY_URL.format(insCode), headers=request_headers, cookies=cookie_jar
    )
    result.raise_for_status()  # raises exception when not a 2xx response
    result = json.loads(result.text)
    data_log.info(f'Identity of "{insCode}" recieved correctly.')
    return flatten_json(result["instrumentIdentity"])

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


################################################################################################
#                                 INSTRUMENT DAILY OHLCV + CSV                                 #
################################################################################################


def get_daily_prices(insCodes:Union[str,list], force_download=False):
    
    if isinstance(insCodes,str):
        insCodes = [insCodes] 
    
    if config.LAST_UPDATE < last_possible_deven():
        force_download = True
    
    if force_download:
        
        daily_prices = {}
        
        for insCode in insCodes:
            resp = requests.get(
                url=f"http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyListCSV/{insCode}/{insCode}",
                headers=request_headers,
                cookies=cookie_jar)
            resp.raise_for_status()
            
            data = StringIO(resp.text)
            df = pd.read_csv(data)
            df = df.set_index('<TICKER>')
            
            df.to_csv(f'./tickers_data/{insCode}.csv')
            
            data_log.info(f'Downloaded Daily Prices for: {insCode}')
            
            daily_prices[insCode] = df
            
            data_log.info(f'Daily Prices CSV Saved for: {insCode}')
    
    else:
        daily_prices = load_prices_csv(insCodes=insCodes)

    return daily_prices


################################################################################################
#                                     LOAD PRICES FROM CSV                                     #
################################################################################################

@calculate_MA(active=True)
def load_prices_csv(insCodes: list) -> dict: 
    try:        
        daily_prices = {insCode: pd.read_csv(
                config.CSV_DATABASE_PATH + "/" + insCode + ".csv",
                dtype={
                    "<LOW>": "float32",
                    "<HIGH>": "float32",
                    "<OPEN>": "float32",
                    # "<VALUE>": "uint64",
                },
            ) for insCode in insCodes}
    
    except (Exception) as e:
            print(f"Error: {e}")
    
    return daily_prices
