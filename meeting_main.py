"""主程序。"""

import argparse

from meeting_summary_workbook import main_process, stat_time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')

    subparsers = parser.add_subparsers(dest='subparser_name')
    parser_stat_time = subparsers.add_parser('stat_time', help='统计参会时长')
    parser_stat_time.add_argument('meeting')

    parser_stat_absent = subparsers.add_parser('stat_absent', help='统计缺勤人数')
    parser_stat_absent.add_argument('meeting')

    args = parser.parse_args()

    if args.subparser_name is None:
        parser.print_help()
        return

    main_process(args)


if __name__ == '__main__':
    main()
