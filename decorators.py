from functools import wraps

def calculate_MA(active=False):
    def decorate(fn):
        if active:
            @wraps(fn)
            def wrapper(*args, **kwargs):
                daily_prices = fn(*args, **kwargs)
                for insCode in daily_prices.keys():
                    daily_prices[insCode]['<VOL> MA 3'] = daily_prices[insCode]['<VOL>'].rolling(3, min_periods=1).mean()
                    daily_prices[insCode]['<VOL> MA 5'] = daily_prices[insCode]['<VOL>'].rolling(5, min_periods=1).mean()
                    daily_prices[insCode]['<VOL> MA 9'] = daily_prices[insCode]['<VOL>'].rolling(9, min_periods=1).mean()
                    daily_prices[insCode]['<VOL> MA 26'] = daily_prices[insCode]['<VOL>'].rolling(26, min_periods=1).mean()
                    daily_prices[insCode]['<VOL> MA 52'] = daily_prices[insCode]['<VOL>'].rolling(52, min_periods=1).mean()
                return daily_prices
            return wrapper
        else:
            return fn
    return decorate

def clean_data():
    pass