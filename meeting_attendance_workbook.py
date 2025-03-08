#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""考勤数据工作簿。"""

import re
from datetime import datetime, timedelta
from functools import partial, reduce
from itertools import groupby
from operator import add, attrgetter, methodcaller
from typing import NamedTuple, Tuple

from meeting_comm import (
    InvalidAttendanceInfo,
    pipe, swap_args, tuple_args, expand_groupby,
)


OVERVIEW_OF_MEMBER_ATTENDANCE = '成员参会概况'
DETAIL_OF_MEMBER_ATTENDANCE = '成员观看明细'


class AttendanceInfo(NamedTuple):
    """参会信息。"""
    nickname: str  # 会议昵称
    meeting_name: str  # 会议名称
    origin_name: str
    enter_time: datetime
    exit_time: datetime


AttendanceInfos = Tuple[AttendanceInfo, ...]


USERNAME_REGEX = re.compile(r'(.*)\((.+)\)$')


def get_meeting_name(fullname: str) -> str:
    """获取会议名称。"""
    matchobj = USERNAME_REGEX.match(fullname)
    if not matchobj:
        raise InvalidAttendanceInfo(f'获取会议名称错误：{fullname}')
    if matchobj.group(1):
        return matchobj.group(1)
    return fullname


def get_nickname(fullname: str) -> str:
    """获取用户昵称。"""
    matchobj = USERNAME_REGEX.match(fullname)
    if not matchobj:
        raise InvalidAttendanceInfo(f'获取用户昵称错误：{fullname}')
    return matchobj.group(2)


def parse_fullname(fullname: str) -> Tuple[str, str]:
    """解析用户名。"""
    matchobj = USERNAME_REGEX.match(fullname)
    if not matchobj:
        raise InvalidAttendanceInfo(f'解析用户名错误：{fullname}')

    if matchobj.group(1):
        meeting_name = matchobj.group(1)
    else:
        meeting_name = fullname
    return meeting_name, matchobj.group(2)


# 标准化用户昵称
# str -> str
normalize_name = pipe(
    partial(re.sub, r' |_|-|，|~|', ''),
    partial(re.sub, r'\d+', pipe(methodcaller('group', 0), int, str)),
    partial(re.sub, r'[Ａ-Ｚａ-ｚ０-９！-～]', lambda x: chr(ord(x.group(0)) - 65248)), # 全角字符转半角
)

# 城市映射
CITY_MAPPING = {
    '厦门': '厦',
    '杭州': '杭',
    '福州': '福',
    '北京': '京',
}


def normalize_nickname(nickname: str) -> str:
    """标准化用户昵称。"""
    if len(nickname) < 2:
        return nickname
    if nickname[0] == '卾':
        first = '鄂'
    else:
        first = nickname[0]
    if nickname[:2] == '厦门':
        first = '厦'
        second = nickname[2]
        thrid = nickname[3:]
    else:
        second = nickname[1]
        thrid = nickname[2:]
    return first + second.upper() + thrid


# 解析时间
# str -> datetime
parse_datetime = pipe(
    partial(swap_args(datetime.strptime), '%Y-%m-%d %H:%M:%S'),
)

# 解析时间
# str -> time
parse_time = pipe(
    partial(swap_args(datetime.strptime), '%H:%M:%S'),
    methodcaller('time'),
)

# 转换“成员参会明细”表为内部数据结构
# Worksheet -> Tuple[Tuple[str, ...], ...]
convert_detail_sheet = pipe(
    methodcaller('iter_rows', min_row=10, min_col=2, max_col=9, values_only=True),
    tuple,
)


def transform_row_data(row: Tuple[str, ...]) -> Tuple[str, ...]:
    """转换行数据。"""
    if row[0]:
        return row
    return '(None)', *row[1:]


def parse_attendance_info(row: Tuple[str, ...]):
    """解析参会信息。"""
    fullname = row[0]
    meeting_name, nickname = parse_fullname(fullname)

    return AttendanceInfo(
        normalize_nickname(normalize_name(nickname)),
        meeting_name,
        fullname,
        parse_datetime(row[5]),
        parse_datetime(row[6]),
    )


# 解析成员参会明细条目
# Tuple[str, ...] -> Tuple[AttendanceInfo, ...]
parse_attendance_detail_info = pipe(
    tuple_args,
    partial(
        map,
        transform_row_data,
    ),
    partial(
        map,
        parse_attendance_info,
    ),
    tuple,
)


def merge_attendance_infos(attendance_infos: AttendanceInfos) -> dict[str, AttendanceInfos]:
    """合并同名的参会信息。"""
    return dict(
        expand_groupby(groupby(attendance_infos, key=attrgetter('origin_name')))
    )


def partition_attendance_infos(attendance_infos: AttendanceInfos) -> dict[str, AttendanceInfos]:
    """划分参会信息。"""
    return dict(
        expand_groupby(
            groupby(
                sorted(attendance_infos, key=attrgetter('meeting_name')),
                key=attrgetter('meeting_name')
            )
        )
    )


# 解析“成员参会明细”
# Worksheet -> Tuple[AttendanceInfo, ...]
parse_attendance_detail_sheet = pipe(
    convert_detail_sheet, parse_attendance_detail_info
)


def does_attendance_detail_info_intersect(meeting_start_time: datetime,
                                          meeting_end_time: datetime,
                                          info: AttendanceInfo) -> bool:
    """参会明细信息是否与会议时间相交。"""
    if info.enter_time > meeting_end_time:
        return False
    if info.exit_time < meeting_start_time:
        return False
    return True


def normalize_attendance_detail_info_time(meeting_start_time: datetime,
                                          meeting_end_time: datetime,
                                          info: AttendanceInfo) -> AttendanceInfo:
    """标准化参会明细信息中的时间。"""
    if info.enter_time < meeting_start_time:
        info = info._replace(enter_time=meeting_start_time)
    if info.exit_time > meeting_end_time:
        info = info._replace(exit_time=meeting_end_time)
    return info


def get_attendance_time_by_detail_info(info: AttendanceInfo) -> timedelta:
    """通过参会信息获取出席时间。"""
    return info.exit_time - info.enter_time


def summarize_attendance_time(attendance_infos: AttendanceInfos) -> timedelta:
    """汇总获取出席时间。"""
    return reduce(
        add, map(get_attendance_time_by_detail_info, attendance_infos), timedelta()
    )
