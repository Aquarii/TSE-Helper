from functools import wraps

def calculate_MA(fn):
    
    @wraps(fn)
    def wrapper(*args, **kwargs):
        
        daily_prices = fn(*args, **kwargs)
        
        for insCode in daily_prices.keys():
            
            # Volume MA
            daily_prices[insCode]['<VOL> MA 3'] = daily_prices[insCode]['<VOL>'].rolling(3, min_periods=1).mean()
            daily_prices[insCode]['<VOL> MA 5'] = daily_prices[insCode]['<VOL>'].rolling(5, min_periods=1).mean()
            daily_prices[insCode]['<VOL> MA 9'] = daily_prices[insCode]['<VOL>'].rolling(9, min_periods=1).mean()
            daily_prices[insCode]['<VOL> MA 26'] = daily_prices[insCode]['<VOL>'].rolling(26, min_periods=1).mean()
            daily_prices[insCode]['<VOL> MA 52'] = daily_prices[insCode]['<VOL>'].rolling(52, min_periods=1).mean()
            daily_prices[insCode]['<VOL> MA 90'] = daily_prices[insCode]['<VOL>'].rolling(90, min_periods=1).mean()
            
            # Value MA
            # daily_prices[insCode]['<VALUE> MA 3'] = daily_prices[insCode]['<VALUE>'].rolling(3, min_periods=1).mean()
            # daily_prices[insCode]['<VALUE> MA 5'] = daily_prices[insCode]['<VALUE>'].rolling(5, min_periods=1).mean()
            # daily_prices[insCode]['<VALUE> MA 9'] = daily_prices[insCode]['<VALUE>'].rolling(9, min_periods=1).mean()
            # daily_prices[insCode]['<VALUE> MA 26'] = daily_prices[insCode]['<VALUE>'].rolling(26, min_periods=1).mean()
            # daily_prices[insCode]['<VALUE> MA 52'] = daily_prices[insCode]['<VALUE>'].rolling(52, min_periods=1).mean()
            # daily_prices[insCode]['<VALUE> MA 90'] = daily_prices[insCode]['<VALUE>'].rolling(90, min_periods=1).mean()
            
        return daily_prices
    
    return wrapper


def clean_data(remove_days_with_no_trades=True):
    
    def decorate(fn):
        
        @wraps(fn)
        def wrapper(*args, **kwargs):
            
            prices = fn(*args, **kwargs)
            
            for insCode in list(prices.keys()):
                
                # Remove tickers with no history
                if len(prices[insCode].index):
                    
                    prices[insCode].set_index('<DTYYYYMMDD>', inplace=True)
                    prices[insCode].sort_index(ascending=True, inplace=True)
                    prices[insCode].drop(['<PER>','<TICKER>'], axis=1, inplace=True)
                    
                    # Remove days with no trade
                    if remove_days_with_no_trades:
                        
                        prices[insCode] = prices[insCode][prices[insCode]['<OPENINT>'] != 0]
                
                if not len(prices[insCode].index):
                    
                    del prices[insCode]
            
            return prices
        
        return wrapper
    
    return decorate