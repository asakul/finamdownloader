# -*- coding: utf-8 -*-

from pandas import DataFrame, read_csv, ExcelWriter
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from datetime import datetime, timedelta, date
from time import sleep
import codecs
import sys


finam_symbols = None 
periods = {'tick': 1, '1min': 2, '5min': 3, '10min': 4, '15min': 5,
           '30min': 6, 'hour': 7, 'daily': 8, 'week': 9, 'month': 10}

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

__all__ = ['periods', 'date_formats', 'time_formats', 'field_separators', 'get_quotes_finam', 'get_symbols_list']

def download_finam_symbols():
    global finam_symbols
    if not finam_symbols:
        finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
    

class Params:
    def __init__(self, period, date_fmt = date_formats['yyyymmdd'], time_fmt = time_formats['hhmmss'], field_separator = field_separators[','], include_header = True):
        self.period = period
        self.date_format = date_fmt
        self.time_format = time_fmt
        self.field_separator = field_separator
        self.include_header = include_header


def __get_finam_code__(symbol):
    s_id = str(finam_symbols[0])
    s_code = str(finam_symbols[2])
    star = str(s_code).find("[\'") + 2
    en = s_code.find("\']")
    names = s_code[star : en].split('\',\'')
    ids = s_id[s_id.find('[') + 1 : s_id.find(']')].split(',')
    if symbol in names:
        max_id = 0
        for i, name in enumerate(names):
            if name == symbol and i > max_id:
                max_id = i
        return int(ids[max_id])
    else:
        raise Exception("%s not found\r\n" % symbol)


def __get_url__(symbol, params, start_date, end_date):
    include_header = 0
    if params.include_header:
        include_header = 1

    finam_HOST = "195.128.78.52"
    finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf={0}&tmf={1}&MSOR=0&mstime=on&mstimever=1&sep={2}&sep2=1&at={3}&".format(params.date_format, params.time_format, params.field_separator, include_header)
    symb = __get_finam_code__(symbol)
    request_params = urlencode({"p": params.period, "em": symb,
                        "df": start_date.day, "mf": start_date.month - 1,
                        "yf": start_date.year,
                        "dt": end_date.day, "mt": end_date.month - 1,
                        "yt": end_date.year, "cn": symbol})

    stock_URL = finam_URL + request_params
    if params.period == periods['tick']:
        return "http://" + finam_HOST + stock_URL + '&code='+ symbol + '&datf=11'
    else:
        return "http://" + finam_HOST + stock_URL + '&datf=1'


def __get_tick_quotes_finam__(_symbol, start_date, end_date):
    """
    Return downloaded tick quotes.
    """
    start_date = datetime.strptime(start_date, "%Y%m%d").date()
    end_date = datetime.strptime(end_date, "%Y%m%d").date()
    delta = end_date - start_date
    data = DataFrame()
    try:
        for i in range(delta.days + 1):
            day = timedelta(i)
            # exclude weekends
            if (start_date + day).weekday() == 5 or (start_date + day).weekday() == 6:
                continue

            url = __get_url__(_symbol, periods['tick'], start_date + day, start_date + day)
            req = Request(url)
            req.add_header('Referer', 'http://www.finam.ru/analysis/profile0000300007/default.asp')
            r = urlopen(req)
            try:
                #tmp_data = read_csv(r, sep=';').sort_index() # separate index: date and time
                tmp_data = read_csv(r, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
                if data.empty:
                    data = tmp_data
                else:
                    data = data.append(tmp_data)
            except ValueError as e:
                if str(e) == 'No columns to parse from file':
                    print('no data: {} {}'.format(_symbol, start_date + day))
                else:
                    print("error: ", e)
                    #raise
                sleep(2)  # to avoid ban :)
            except Exception as ex:
                print('downloading error: {} {}  = {} {}'.format(_symbol, start_date + day, sys.exc_info()[0], ex))

    except Exception as e:
        print(e)

    data.columns = ['Last', 'Volume', 'Id']
    data['Symbol'] = _symbol
    return data

def __split_dates(period, start_date, end_date):
    result = []
    splits = {periods['daily'] : timedelta(15 * 365),
            periods['hour'] : timedelta(3 * 365),
            periods['30min'] : timedelta(2 * 365),
            periods['15min'] : timedelta(2 * 365),
            periods['10min'] : timedelta(2 * 365),
            periods['5min'] : timedelta(1 * 365),
            periods['1min'] : timedelta(1 * 365) }

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
        print(date_list)
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
    if params.period == periods['tick']:
        raise Exception
    else:
        return __get_raw_timeframe_finam__(symbol, params, start_date, end_date)

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

    result = zip(codes, names)
    return result


