"""主程序。"""

import argparse

from meeting_summary_workbook import main_process


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('meeting')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    main_process(args)


if __name__ == '__main__':
    main()
