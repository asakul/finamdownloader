# -*- coding: utf-8 -*-

from urllib.parse import urlencode
from urllib.request import urlopen, Request
from datetime import datetime, timedelta, date
from time import sleep
import codecs
import sys


finam_symbols = None 
periods = {'tick': 1, '1min': 2, '5min': 3, '10min': 4, '15min': 5,
           '30min': 6, 'hour': 7, 'daily': 8, 'week': 9, 'month': 10}

finam_markets = { 200 : 'МосБиржа топ',
    1 : 'МосБиржа акции',
    14 : 'МосБиржа фьючерсы', 41: 'Курс рубля', 45: 'МосБиржа валютный рынок',
    2: 'МосБиржа облигации', 12: 'МосБиржа внесписочные облигации', 29: 'МосБиржа пифы',
    8: 'Расписки', 6: 'Мировые Индексы', 24: 'Товары', 5: 'Мировые валюты', 25: 'Акции США(BATS)', 7: 'Фьючерсы США', 27: 'Отрасли экономики США',
    26: 'Гособлигации США', 28: 'ETF', 30: 'Индексы мировой экономики', 3: 'РТС', 20: 'RTS Board', 10: 'РТС-GAZ', 17: 'ФОРТС Архив',
    31: 'Сырье Архив', 38: 'RTS Standard Архив', 16: 'ММВБ Архив', 18: 'РТС Архив', 9: 'СПФБ Архив', 32: 'РТС-BOARD Архив',
    39: 'Расписки Архив', -1: 'Отрасли'}

date_formats = {'yyyymmdd' : 1,
        'yymmdd' : 2,
        'ddmmyy' : 3,
        'dd/mm/yy' : 4,
        'mm/dd/yy' : 5 }

time_formats = {'hhmmss' : 1,
        'hhmm' : 2,
        'hh:mm:ss' : 3,
        'hh:mm' : 4 }

field_separators = {',' : 1,
        '.' : 2,
        ';' : 3,
        'tab' : 4,
        'space' : 5 }

archives = [3, 16, 17, 18, 31, 32, 38, 39, 517]

__all__ = ['periods', 'date_formats', 'time_formats', 'field_separators', 'get_quotes_finam', 'get_symbols_list', 'get_markets_list']

def download_finam_symbols():
    global finam_symbols
    if not finam_symbols:
        finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
    

class Params:
    def __init__(self, period, date_fmt = date_formats['yyyymmdd'], time_fmt = time_formats['hhmmss'], field_separator = field_separators[','], include_header = True, fill_empty = False):
        self.period = period
        self.date_format = date_fmt
        self.time_format = time_fmt
        self.field_separator = field_separator
        self.include_header = include_header
        self.force_market = None
        self.fill_empty = fill_empty


def __get_finam_code__(symbol, force_market=None):
    symbols = get_symbols_list()
    for (code, _, id_, market, _) in symbols:
        if code == symbol:
            if force_market and force_market == market:
                return (id_, market)
            if not force_market and market in archives: # Skip RTS
                continue
            return (id_, market)
    else:
        raise Exception("%s not found\r\n" % symbol)


def __get_url__(symbol, params, start_date, end_date):
    include_header = 0
    if params.include_header:
        include_header = 1
    
    force_market = None
    try:
        force_market = params.force_market
    except KeyError:
        pass
        
    (symb, market) = __get_finam_code__(symbol, force_market)

    finam_HOST = "195.128.78.52"
    finam_URL = "/export9.out?market={0}&f={5}&e=.csv&dtf={1}&tmf={2}&MSOR=0&mstime=on&mstimever=1&sep={3}&sep2=1&at={4}&".format(market, params.date_format, params.time_format, params.field_separator, include_header, symbol)
    if params.fill_empty:
        finam_URL += 'fsp=1&'

    request_params = urlencode({"p": params.period, "em": symb,
                        "df": start_date.day, "mf": start_date.month - 1,
                        "yf": start_date.year,
                        "dt": end_date.day, "mt": end_date.month - 1,
                        "yt": end_date.year, "code": symbol})

    stock_URL = finam_URL + request_params
    if params.period == periods['tick']:
        return "http://" + finam_HOST + stock_URL + '&code='+ symbol + '&datf=11'
    else:
        return "http://" + finam_HOST + stock_URL + '&datf=1'



def __split_dates(period, start_date, end_date):
    result = []
    splits = {periods['daily'] : timedelta(15 * 365),
            periods['hour'] : timedelta(3 * 365),
            periods['30min'] : timedelta(2 * 365),
            periods['15min'] : timedelta(2 * 365),
            periods['10min'] : timedelta(2 * 365),
            periods['5min'] : timedelta(1 * 365),
            periods['1min'] : timedelta(1 * 365),
            periods['tick'] : timedelta(1)
            }

    if period == periods['month'] or period == periods['week']:
        return [(start_date, end_date)]
    else:
        delta = splits[period]
        s = start_date
        while s < end_date:
            e = min(s + delta, end_date)
            result.append((s, e))
            s = e + timedelta(1)

    return result

def __get_raw_timeframe_finam__(_symbol, params, start_date, end_date):
    result = b""
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    date_list = __split_dates(params.period, start_date, end_date)
    url = __get_url__(_symbol, params, date_list[0][0], date_list[0][1])
    p = urlopen(url)
    result += p.read()
    date_list.pop(0)
    params.include_header = False
    while len(date_list) > 0:
        sleep(2)  # to avoid ban :)
        (s, e) = date_list[0]
        date_list.pop(0)
        url = __get_url__(_symbol, params, s, e)
        p = urlopen(url)
        result += p.read()

    return result

def get_raw_quotes_finam(symbol, params, start_date, end_date=date.today().strftime("%Y%m%d")):
    """
    Return downloaded prices for symbol from start date to end date with default period daily
    Date format = YYYYMMDD
    Period can be in ['tick','1min','5min','10min','15min','30min','hour','daily','week','month']
    """
    download_finam_symbols()
    return __get_raw_timeframe_finam__(symbol, params, start_date, end_date)

def get_or_default(a_list, key, default_value):
    try:
        return a_list[key]
    except KeyError:
        return default_value

def get_symbols_list():
    download_finam_symbols()
    s_code = str(finam_symbols[2])
    star = str(s_code).find("[\'") + 2
    en = s_code.find("\']")
    codes = s_code[star : en].split('\',\'')

    s_name = codecs.decode(finam_symbols[1], "cp1251")
    star = str(s_name).find("[\'") + 2
    en = s_name.find("\']")
    names = s_name[star : en].split('\',\'')
    
    s_id = codecs.decode(finam_symbols[0], "cp1251")
    star = str(s_id).find("[") + 1
    en = s_id.find("]")
    ids = s_id[star : en].split(',')
    
    s_markets = codecs.decode(finam_symbols[3], "cp1251")
    star = str(s_markets).find("[") + 1
    en = s_markets.find("]")
    markets_s = s_markets[star : en].split(',')

    markets = list(map(lambda x: int(x), markets_s))
    market_names = list(map(lambda x: get_or_default(finam_markets, x, ""), markets))
    
    result = zip(codes, names, ids, markets, market_names)
    return result

def get_markets_list():
    return finam_markets
        
