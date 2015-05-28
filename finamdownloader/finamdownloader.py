
import finam.finamquote_dwl as f
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Finam quote downloader')
    parser.add_argument('-s', '--symbol', action='store', dest='symbol', help='Ticker symbol to download', required=True)
    parser.add_argument('-f', '--from', action='store', dest='date_from', help='Starting date in YYYYMMDD format', required=True)
    parser.add_argument('-t', '--to', action='store', dest='date_to', help='Ending date in YYYYMMDD format')
    parser.add_argument('-o', '--output', action='store', dest='output', help='Target file ("-" means stdout)')

    periods = ", ".join(f.periods.keys())

    parser.add_argument('-p', '--period', action='store', dest='period', help='Quotes period: can be one of the following: ' + periods)

    args = parser.parse_args()

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
            out = open("{0}_{1}_{2}_{3}.csv".format(args.symbol, args.date_from, args.date_to, args.period), 'w+')
        else:
            out = open(args.output, 'w+')

    out.write(f.get_raw_quotes_finam(args.symbol, f.Params(f.periods[args.period]), args.date_from, args.date_to).decode('utf-8'))


if __name__ == '__main__':
    main()

