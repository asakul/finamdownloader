from pandas import DataFrame, read_csv, ExcelWriter
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from datetime import datetime, timedelta, date
#import numpy as np
from time import sleep
import sys


finam_symbols = urlopen('http://www.finam.ru/cache/icharts/icharts.js').readlines()
periods = {'tick': 1, '1min': 2, '5min': 3, '10min': 4, '15min': 5,
           '30min': 6, 'hour': 7, 'daily': 8, 'week': 9, 'month': 10}

__all__ = ['periods', 'get_quotes_finam']


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


def __get_url__(symbol, period, start_date, end_date):
    finam_HOST = "195.128.78.52"
    #'http://195.128.78.52/table.csv?market=1&em=3&code=SBER&df=9&mf=11&yf=2013&dt=9&mt=11&yt=2013&p=1&f=table&e=.csv&cn=SBER&dtf=1&tmf=1&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&datf=9&at=1'
    #'http://195.128.78.52/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&em=20509&p=1&mf=10&cn=FEES&mt=10&df=22&dt=22&yt=2013&yf=2013&datf=11'
    #finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=1&MSOR=0&sep=1&sep2=1&at=1&"
    finam_URL = "/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1&"
    #'/table.csv?d=d&market=1&f=table&e=.csv&dtf=1&tmf=3&MSOR=0&mstime=on&mstimever=1&sep=3&sep2=1&at=1'
    symb = __get_finam_code__(symbol)
    params = urlencode({"p": period, "em": symb,
                        "df": start_date.day, "mf": start_date.month - 1,
                        "yf": start_date.year,
                        "dt": end_date.day, "mt": end_date.month - 1,
                        "yt": end_date.year, "cn": symbol})

    stock_URL = finam_URL + params
    if period == periods['tick']:
        return "http://" + finam_HOST + stock_URL + '&code='+ symbol + '&datf=11'
    else:
        return "http://" + finam_HOST + stock_URL + '&datf=5'


def __period__(s):
    return periods[s]


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


def __get_timeframe_finam__(_symbol, start_date, end_date, period):
    try:
        start_date = datetime.strptime(start_date, "%Y%m%d").date()
        end_date = datetime.strptime(end_date, "%Y%m%d").date()
        url = __get_url__(_symbol, __period__(period), start_date, end_date)
        pdata = read_csv(url, index_col=0, parse_dates={'index': [0, 1]}, sep=';').sort_index()
    except Exception as e:
        print(e)

    pdata.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    #[symbol + '.' + i for i in ['Open', 'High', 'Low', 'Close', 'Volume']]
    pdata['Symbol'] = _symbol
    return pdata


def get_quotes_finam(symbol, start_date='20150101', end_date=date.today().strftime("%Y%m%d"),
                     period='daily'):
    """
    Return downloaded prices for symbol from start date to end date with default period daily
    Date format = YYYYMMDD
    Period can be in ['tick','1min','5min','10min','15min','30min','hour','daily','week','month']
    """
    if __period__(period) == periods['tick']:
        return __get_tick_quotes_finam__(symbol, start_date, end_date)
    else:
        return __get_timeframe_finam__(symbol, start_date, end_date, period)


def save_data(_code, _start_date, _end_date, _per):
    """
    each instrument will be saved in separate excel file.
    """
    clean_code = _code.replace(" ", "").split(',')

    for y in clean_code:
        print('download %s data for %s' % (_per, y))
        quote = get_quotes_finam(symbol=y, start_date=_start_date, end_date=_end_date, period=_per)
        print(quote.head(n=3))

        #C:\\Users\\login\\PycharmProjects\\trade\\
        url = '{0}.xlsx'.format(y+"_"+_start_date+"_"+_end_date)
        try:
            with ExcelWriter(url) as writer:
                quote.to_excel(writer, y)
                #quote.to_excel(writer, 'Data 1')     #write to the second list
                print(y + ' saved to file')
        except Exception as e:
            print("save to file error: ", sys.exc_info()[0], e)


def __save_to_one_file__(_code, _start_date, _end_date, _per):
    """
    Save all quotes to 1 excel file
    """
    clean_code = _code.replace(" ", "").split(',')
    url = '{0}.xlsx'.format(date.today().strftime("%Y%m%d"))
    _df = DataFrame()  # (np.random.randn(1, 6), columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    for y in clean_code:
        try:
            quote = get_quotes_finam(y, start_date=_start_date, end_date=_end_date, period=_per)
            _df = _df.append(quote[:])
        except Exception as e:
            print("could not find an instrument: {0}".format(y), sys.exc_info()[0], e)
            continue

    print(_df.head(n=5))

    with ExcelWriter(url) as writer:
        try:
            _df.to_excel(writer)

        except PermissionError as ex:
            print(ex)
        except Exception as e:
            print("save to file error:", sys.exc_info()[0], e)
        else:
            print("Finish")


if __name__ == "__main__":
    code = 'SiM5'
    start ='20141201'
    end = '20150413'
    per = 'tick'

    save_data(code, start, end, per)
#+++++++++++++++++++++++++++++++++++++++++++++++=
    #__save_to_one_file__(code, start, end, per)