import logging


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Logging Setup ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def setup_logger(log_file_path, formatter:logging.Formatter, name='__name__', level=logging.INFO):
    handler = logging.FileHandler(log_file_path, 'a')        
    handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    logger.addHandler(handler)
    
    return logger

formatter_app = logging.Formatter('%(asctime)s >> %(levelname)s >> %(filename)s >> %(lineno)d >> %(message)s')
formatter_activity = logging.Formatter('%(asctime)s >> %(message)s')

# TSETMC Activity Logger
data_log = setup_logger('data_io.log', formatter_activity, name='activity')
# App Debugging Logger
debugging_log = setup_logger('debug.log', formatter_app, name='app')


#▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬ Parsi Tools ▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬#

def ar_to_fa(string: str):
    return string.replace('ك','ک').replace('ي', 'ی').replace('\u200c',' ').replace('_',' ').strip()

def fa_to_ar(string: str):
    return string.replace('ک', 'ك').replace('ی', 'ي').replace('\u200c',' ').replace('_',' ').strip()

def ar_to_fa_series(series):
    return series.str.replace('ك','ک').str.replace('ي', 'ی').str.replace('\u200c',' ').str.replace('_',' ').str.strip()

def fa_to_ar_series(series):
    return series.str.replace('ک','ك').str.replace('ی', 'ي').str.replace('\u200c',' ').str.replace('_',' ').str.strip()
