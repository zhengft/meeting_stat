#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from datetime import datetime, time, timedelta
from functools import partial, reduce
from itertools import chain, filterfalse, groupby, repeat
from operator import add, attrgetter, contains, itemgetter, methodcaller, not_
from typing import Iterator, NamedTuple, Tuple

from meeting_comm import (
    Cell, InvalidAttendanceInfo,
    constant, cross, debug, dispatch, identity, if_, pipe, raise_, starapply,
    swap_args, to_stream, tuple_args,
    expand_groupby,
)


OVERVIEW_OF_MEMBER_ATTENDANCE = '成员参会概况'
DETAIL_OF_MEMBER_ATTENDANCE = '成员观看明细'


class AttendanceInfo(NamedTuple):
    """参会信息。"""
    nickname: str
    norm_name: str
    origin_name: str
    enter_time: datetime
    exit_time: datetime


AttendanceInfos = Tuple[AttendanceInfo, ...]


# 创建原始的出席信息
create_origin_attendance_info = pipe(
    tuple_args,
    starapply(AttendanceInfo),
)


USERNAME_REGEX = re.compile(r'.*\((.+)\)')


def add_time(a: time, b: time) -> time:
    """时间相加。"""
    second = a.second + b.second
    minute = a.minute + b.minute + second // 60
    hour = a.hour + b.hour + minute // 60
    return time(hour, minute % 60, second % 60)


def get_nickname(fullname: str) -> str:
    """获取用户昵称。"""
    matchobj = USERNAME_REGEX.match(fullname)
    if not matchobj:
        raise InvalidAttendanceInfo(f'获取用户昵称错误：{fullname}')
    return matchobj.group(1)


# 标准化用户昵称
# str -> str
normalize_name = pipe(
    partial(re.sub, r' |_|-|，', ''),
    partial(re.sub, r'\d+', pipe(methodcaller('group', 0), int, str)),
)


def normalize_nickname(nickname: str) -> str:
    """标准化用户昵称。"""
    if len(nickname) < 2:
        return nickname
    return nickname[0] + nickname[1].upper() + nickname[2:]


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


# 转换“成员参会概况”表为内部数据结构
# Worksheet -> Tuple[Tuple[Cell, ...], ...]
convert_overview_sheet = pipe(
    methodcaller('iter_rows', min_row=10, max_col=5, values_only=True),
    partial(map, pipe(partial(map, Cell), tuple)),
    tuple,
)

# 转换“成员参会明细”表为内部数据结构
# Worksheet -> Tuple[Tuple[str, ...], ...]
convert_detail_sheet = pipe(
    methodcaller('iter_rows', min_row=10, min_col=2, max_col=9, values_only=True),
    tuple,
)

# 解析人员信息
# Tuple[Cell, ...] -> Iterator[AttendanceInfo]
parse_attendance_info = pipe(
    tuple_args,
    dispatch(
        itemgetter(0),
        itemgetter(0),
        itemgetter(4),
    ),
    partial(map, attrgetter('value')),
    cross(
        pipe(get_nickname, partial(re.split, r'＆|&')),
        repeat,
        repeat,
    ),
    starapply(zip),
    partial(
        map,
        cross(
            normalize_name,
            identity,
            parse_time,
        ),
    ),
    partial(map, create_origin_attendance_info),
)

# 解析成员参会明细条目
# Tuple[str, ...] -> Tuple[AttendanceInfo, ...]
parse_attendance_detail_info = pipe(
    tuple_args,
    partial(
        map,
        pipe(
            dispatch(
                itemgetter(0),
                itemgetter(0),
                itemgetter(0),
                itemgetter(5),
                itemgetter(6)
            ),
            tuple,
        ),
    ),
    partial(filter, pipe(itemgetter(0), bool)),
    partial(
        map,
        pipe(
            cross(
                pipe(get_nickname, normalize_name, normalize_nickname),
                pipe(normalize_name, normalize_nickname),
                identity,
                parse_datetime,
                parse_datetime,
            ),
            tuple,
        ),
    ),
    partial(
        map,
        starapply(AttendanceInfo),
    ),
    tuple,
)


def merge_attendance_info(left: AttendanceInfo,
                          right: AttendanceInfo) -> AttendanceInfo:
    """合并同名的参会信息。"""
    if len(left.nickname) < len(right.nickname):
        final = right
    else:
        final = left

    return AttendanceInfo(
        final.nickname,
        final.origin_name,
        add_time(left.attendance_time, right.attendance_time),
        True
    )


def merge_attendance_infos(attendance_infos: AttendanceInfos) -> dict[str, AttendanceInfos]:
    """合并同名的参会信息。"""
    return dict(
        expand_groupby(groupby(attendance_infos, key=attrgetter('origin_name')))
    )


# 解析参会页签
parse_attendance_sheet = lambda convert_func, parse_func: pipe(
    convert_func,
    partial(map, parse_func),
    chain.from_iterable,
    partial(sorted, key=attrgetter('nickname')),
    partial(groupby, key=attrgetter('nickname')),
    partial(
        map,
        pipe(
            itemgetter(1),
            partial(reduce, merge_attendance_info),
        )
    ),
    tuple,
    dispatch(
        pipe(
            partial(filter, attrgetter('raw_name')),
            partial(sorted, key=attrgetter('raw_name')),
            partial(groupby, key=attrgetter('raw_name')),
            partial(
                map,
                pipe(
                    itemgetter(1),
                    tuple,
                    if_(
                        pipe(
                            partial(
                                map,
                                pipe(
                                    dispatch(
                                        attrgetter('nickname'),
                                        attrgetter('raw_name'),
                                    ),
                                    starapply(contains),
                                )
                            ),
                            all,
                        ),
                        pipe(
                            partial(reduce, merge_attendance_info),
                            to_stream,
                        ),
                    ),
                ),
            ),
            chain.from_iterable,
        ),
        partial(filterfalse, attrgetter('raw_name')),
    ),
    chain.from_iterable,
    partial(sorted, key=attrgetter('nickname')),
    tuple,
)


# 解析“成员参会概况”
# Worksheet -> Tuple[AttendanceInfo, ...]
parse_attendance_overview_sheet = parse_attendance_sheet(
    convert_overview_sheet, parse_attendance_info
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