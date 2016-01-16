
import finam.finamquote_dwl as f
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Finam quote downloader')
    parser.add_argument('-s', '--symbol', action='store', dest='symbol', help='Ticker symbol to download (pass "?" to list available symbols)')
    parser.add_argument('-f', '--from', action='store', dest='date_from', help='Starting date in YYYYMMDD format')
    parser.add_argument('-t', '--to', action='store', dest='date_to', help='Ending date in YYYYMMDD format')
    parser.add_argument('-o', '--output', action='store', dest='output', help='Target file ("-" means stdout, "!" will create filename automatically)')
    parser.add_argument('-m', '--market', action='store', dest='market', help='Force market ("?" will list all available markets and their ids)')
    parser.add_argument('-l', '--fill-empty', action='store_true', dest='fill_empty', help='Fill empty periods')

    periods = ", ".join(f.periods.keys())

    parser.add_argument('-p', '--period', action='store', dest='period', help='Quotes period: can be one of the following: ' + periods)

    args = parser.parse_args()

    if args.symbol == '?':
        syms = f.get_symbols_list()
        for (code, name, id_, market, market_name) in syms:
            print("{0} : {1} : {2}, {3} ({4})".format(id_, code, name, market_name, market))
        return 1
    
    if args.market == '?':
        markets = f.get_markets_list()
        for k in markets:
            print("{0} : {1}".format(k, markets[k]))
        return 1
    
    if not args.symbol:
        print("Should specify symbol")
        return 1

    if not args.date_from:
        print("Should specify starting date")
        return 1

    if not args.date_to:
        print("Should specify ending date")
        return 1

    if not args.period:
        print("Should specify period")
        return 1

    out = sys.stdout

    if args.output and args.output != '-':
        if args.output == '!':
            out = open("{0}_{1}_{2}_{3}.csv".format(args.symbol, args.date_from, args.date_to, args.period), 'wb+')
        else:
            out = open(args.output, 'wb+')

    params = f.Params(f.periods[args.period])
    if args.market:
        params.force_market = args.market

    if args.fill_empty:
        params.fill_empty = True

    out.write(f.get_raw_quotes_finam(args.symbol, params, args.date_from, args.date_to))


if __name__ == '__main__':
    main()

