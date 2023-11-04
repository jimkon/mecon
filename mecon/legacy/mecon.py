import argparse

from legacy.statements import Transactions
from legacy.statements import FullyTaggedData
from legacy.produce_report import create_report


def read_args():
    parser = argparse.ArgumentParser(description='Create full report.')
    # https://docs.python.org/3/library/argparse.html#:~:text=in%20version%203.9.-,The%20add_argument()%20method,-%C2%B6
    parser.add_argument('-p', '--parse', action='store_true')
    parser.add_argument('-t', '--tag', action='store_true')
    parser.add_argument('-r', '--report', action='store_true')
    parser.add_argument('-a', '--all', action='store_true')
    parser.add_argument("-v", "--verbosity", action="count", default=0, help="increase output verbosity")

    args = vars(parser.parse_args())
    return args


def parse_statements():
    print(f"{' PARSING STATEMENTS ':#^80}")
    Transactions(reset=True)


def tag_data():
    print(f"{' TAGGING STATEMENTS ':#^80}")
    FullyTaggedData(reset_tags=True)


def produce_the_full_report():
    print(f"{' PRELOAD FOR REPORT ':#^80}")
    from mecon import _get_fill_dates
    df = FullyTaggedData.instance().dataframe()
    _get_fill_dates([df['date'].min(), df['date'].max()])

    print(f"{' MAKING REPORT ':#^80}")
    FullyTaggedData.instance()
    create_report()


def main():
    args = read_args()

    all_flag = args['all'] or ( (not args['parse']) and (not args['tag']) and (not args['report']))

    if args['parse'] or all_flag:
        parse_statements()
    if args['tag'] or all_flag:
        tag_data()
    if args['report'] or all_flag:
        produce_the_full_report()


if __name__ == "__main__":
    main()
