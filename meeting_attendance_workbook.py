#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from datetime import datetime, time
from functools import partial, reduce
from itertools import chain, groupby, repeat
from operator import attrgetter, ge, itemgetter, methodcaller, not_
from typing import NamedTuple, Tuple

from meeting_comm import (
    Cell, StatError,
    constant, cross, dispatch, identity, if_, pipe, raise_, starapply,
    swap_args, to_stream, tuple_args
)


OVERVIEW_OF_MEMBER_ATTENDANCE = '成员参会概况'


class AttendanceInfo(NamedTuple):
    """参会信息。"""
    nickname: str
    origin_name: str
    attendance_time: time
    merged: bool


AttendanceInfos = Tuple[AttendanceInfo, ...]


GET_NICKNAME_ERROR = '获取用户昵称错误'


USERNAME_REGEX = re.compile(r'.*\((.+?)\)')


def add_time(a: time, b: time) -> time:
    """时间相加。"""
    second = a.second + b.second
    # breakpoint()
    minute = a.minute + b.minute + second // 60
    hour = a.hour + b.hour + minute // 60
    return time(hour, minute % 60, second % 60)


# 获取用户昵称
# str -> str
get_nickname = pipe(
    USERNAME_REGEX.match,
    if_(
        pipe(bool, not_),
        pipe(constant(StatError(GET_NICKNAME_ERROR)), raise_),
    ),
    methodcaller('group', 1),
)

# 标准化用户昵称
# str -> str
normalize_nickname = pipe(
    partial(re.sub, r' |_|-|，', ''),
    partial(re.sub, r'\d+', pipe(methodcaller('group', 0), int, str)),
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
# Worksheet -> Tuple[Tuple[Cell, ...], ...]
convert_detail_sheet = pipe(
    methodcaller('iter_rows', min_row=10, max_col=5, values_only=True),
    partial(map, pipe(partial(map, Cell), tuple)),
    tuple,
)


# 创建原始的出席信息
create_origin_attendance_info = pipe(
    tuple_args,
    dispatch(
        identity,
        pipe(constant(False), to_stream),
    ),
    chain.from_iterable,
    starapply(AttendanceInfo),
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
            normalize_nickname,
            identity,
            parse_time,
        ),
    ),
    partial(map, create_origin_attendance_info),
)

# 合并同名的参会信息
# Iterator[AttendanceInfo] -> AttendanceInfo
merge_attendance_infos = pipe(
    tuple,
    if_(
        pipe(len, partial(ge, 1)),
        itemgetter(0),
        pipe(
            dispatch(
                pipe(itemgetter(0), attrgetter('nickname')),
                pipe(itemgetter(0), attrgetter('origin_name')),
                pipe(
                    partial(map, attrgetter('attendance_time')),
                    partial(reduce, add_time),
                ),
                constant(True),
            ),
            starapply(AttendanceInfo),
        ),
    ),
)

# 解析“成员参会概况”
# Worksheet -> Tuple[AttendanceInfo, ...]
parse_attendance_sheet = pipe(
    convert_overview_sheet,
    partial(map, parse_attendance_info),
    chain.from_iterable,
    partial(sorted, key=attrgetter('nickname')),
    partial(groupby, key=attrgetter('nickname')),
    partial(map, itemgetter(1)),
    partial(map, merge_attendance_infos),
    tuple,
)
