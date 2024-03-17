#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""生活修行考勤表。"""

import os
import re
from datetime import time
from functools import partial
from itertools import (
    chain, count, filterfalse, groupby, pairwise, repeat, starmap
)
from operator import (
    add, attrgetter, contains, eq, floordiv, ge,
    itemgetter, methodcaller, mod, mul, not_, sub
)
from pprint import pformat
from typing import NamedTuple, Tuple

from openpyxl import load_workbook
from openpyxl.styles import Font

from meeting_attendance_workbook import (
    AttendanceInfo, DETAIL_OF_MEMBER_ATTENDANCE,
    get_nickname, parse_attendance_detail_sheet,
)
from meeting_comm import (
    debug, Cell, Chain, MEETING_SUMMARY_FILENAME, MEETING_SUMMARY_OUTPUT_FILENAME,
    MEETING_ATTENDANCE_FILENAME, SUFFIX_NUMBER, StatError,
    constant, cross, dispatch, ensure, eval_graph, identity, if_, invoke,
    islice_, make_graph, partition, pipe, raise_, side_effect, starapply,
    swap_args, to_stream, tuple_args,
    zip_refs_values,
    save_file
)


PEOPLE_SHEET_NAME = '人员总表'
MEETING_INFO_SHEET_NAME = '参数'
MISMATCHED_SHEET_NAME = '未改名'

TEAM_NAME_REGEX = re.compile(r'(..)组')

BLACK_FONT = Font(color='00000000')
RED_FONT = Font(color='00FF0000')


class PersoneelInfo(NamedTuple):
    """人员信息。"""
    name: str
    group: str  # 大组
    team: str  # 小组
    number: int  # 小组编号

    @property
    def formal_name(self) -> str:
        """正式名称。"""
        return f'{self.team}{self.number}{self.name}'


PersoneelInfos = Tuple[PersoneelInfo, ...]

PersoneelAttendanceInfo = Tuple[PersoneelInfo, AttendanceInfo]

PersoneelAttendanceInfos = Tuple[PersoneelAttendanceInfo, ...]


class TeamAttendanceInfo(NamedTuple):
    """小组出勤信息。"""
    team_info: Tuple[str, str]
    enough_of_time_infos: PersoneelAttendanceInfos
    leak_of_time_infos: PersoneelAttendanceInfos
    absent_infos: PersoneelInfos


TeamAttendanceInfos = dict[str, TeamAttendanceInfo]
GroupAttendanceInfo = TeamAttendanceInfos
GroupAttendanceInfos = dict[str, GroupAttendanceInfo]


class MeetingInfo(NamedTuple):
    """会议信息。"""
    solar_term: str
    meeting_time: time


FillCommand = Tuple[int, int, str, bool]

create_fill_command = tuple


MEETING_INFO_KEYS = {
    '节气名': 'solar_term',
    '会议总时长': 'meeting_time',
}

class TeamLocation(NamedTuple):
    team: str  # 小组名
    lineno: int  # 行号


class ParsePersonnelInfoError(StatError):
    """解析人员信息错误。"""


GET_NAME_ERROR = '获取姓名异常'
GET_GROUP_ERROR = '获取大组异常'
GET_TEAM_ERROR = '获取小组异常'
GET_NUMBER_ERROR = '获取小组编号异常'


# Tuple[Any, SupportsIndex]
item_extract = pipe(
    tuple_args,
    cross(
        itemgetter,
        identity
    ),
    starapply(invoke),
)

# 创建提取函数
# x -> if_(
#     pipe(bool, not_),
#     pipe(constant(ParsePersonnelInfoError(x)), raise_),
# )
make_extract_func = pipe(
    dispatch(
        constant(pipe(bool, not_)),
        pipe(
            dispatch(
                pipe(ParsePersonnelInfoError, constant),
                constant(raise_),
            ),
            starapply(pipe),
        ),
    ),
    starapply(if_)
)

# 提取名称
extract_name = make_extract_func(GET_NAME_ERROR)

# 提取大组
extract_group = make_extract_func(GET_GROUP_ERROR)

# 提取小组
extract_team = make_extract_func(GET_TEAM_ERROR)

# 提取小组编号
extract_team_number = pipe(
    SUFFIX_NUMBER.search,
    if_(
        pipe(bool, not_),
        pipe(constant(ParsePersonnelInfoError(GET_NUMBER_ERROR)), raise_),
    ),
    methodcaller('group', 0),
    int
)

# 解析人员信息
# Tuple[Cell, ...] -> PersoneelInfo
parse_personnel_info = pipe(
    partial(islice_, start=1),
    partial(map, attrgetter('value')),
    cross(extract_name, extract_group, extract_team, extract_team_number),
    starapply(PersoneelInfo),
)

# 转换人员总表为内部数据结构
# Worksheet -> Tuple[Tuple[Cell, ...], ...]
convert_people_sheet = pipe(
    methodcaller('iter_rows', min_row=2, max_col=5, values_only=True),
    partial(map, pipe(partial(map, Cell), tuple)),
    tuple,
)

# 解析人员总表
# Worksheet -> PersoneelInfos
parse_people_sheet = pipe(
    convert_people_sheet,
    partial(filter, pipe(itemgetter(1), attrgetter('value'), bool)),
    partial(map, parse_personnel_info),
    tuple
)

# 转换会议信息为内部数据结构
# Worksheet -> Tuple[Tuple[Cell, ...], ...]
convert_meeting_info_sheet = pipe(
    methodcaller('iter_rows', min_row=1, max_col=2, values_only=True),
    partial(map, pipe(partial(map, Cell), tuple)),
    tuple,
)

# coefficient

# 获取时长充足的时间
# int -> int
get_enough_meeting_time = pipe(
    partial(mul, 2/3),
    partial(swap_args(sub), 10),
    int,
)

# 解析会议时间
# str -> time
parse_meeting_time = pipe(
    int,
    get_enough_meeting_time,
    side_effect(
        pipe(
            '参会时长下限为{0}分钟。'.format,
            print,
        ),
    ),
    dispatch(
        partial(swap_args(floordiv), 60),
        partial(swap_args(mod), 60),
    ),
    starapply(time),
)

# 解析会议信息
# Tuple[Cell, ...] -> Tuple[str, Any]
parse_meeting_info = pipe(
    cross(
        pipe(
            attrgetter('value'),
            MEETING_INFO_KEYS.get,
            partial(ensure, bool),
        ),
        attrgetter('value'),
    ),
    tuple,
    if_(
        pipe(itemgetter(0), partial(eq, 'meeting_time')),
        pipe(
            cross(identity, parse_meeting_time),
            tuple,
        ),
    ),
)

# 解析会议信息工作表
# Worksheet -> MeetingInfo
parse_meeting_info_sheet = pipe(
    convert_meeting_info_sheet,
    partial(map, parse_meeting_info),
    dict,
    starapply(MeetingInfo),
)

# 转换大组出勤表为内部数据结构
# Worksheet -> Tuple[Tuple[Cell, ...], ...]
convert_group_attendance_sheet = pipe(
    methodcaller('iter_rows', min_row=1, max_col=1, values_only=True),
    partial(map, pipe(partial(map, Cell), tuple)),
    tuple,
)

# 出席信息中是否包含人员正式名称
# Tuple[PersoneelInfo, AttendanceInfo] -> bool
# PersoneelInfo, AttendanceInfo -> bool
contains_formal_name = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(1), attrgetter('nickname')),
        pipe(itemgetter(0), attrgetter('formal_name')),
    ),
    starapply(contains),
)

# AttendanceInfos -> Iterator[str]
trans_to_nicknames = pipe(
    partial(map, attrgetter('nickname'))
)

# 人员是否出席
# Tuple[PersoneelInfo, AttendanceInfos] -> bool
is_person_present = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(0), repeat),
        itemgetter(1),
    ),
    starapply(zip),
    partial(map, contains_formal_name),
    any,
)

# 由一对一关系生成一对多关系
# Callable[[A, B], C] -> Callable[[A, Iterator[B]], Iterator[C]]
one_more_relation = lambda func: pipe(
    dispatch(
        pipe(itemgetter(0), repeat),
        itemgetter(1),
    ),
    starapply(zip),
    partial(starmap, func),
)

# 参会信息是否有匹配的人员
# Tuple[AttendanceInfo, PersoneelInfos] -> bool
is_attendance_matched = pipe(
    tuple_args,
    one_more_relation(swap_args(contains_formal_name)),
    any,
)

# 划分出席人员与缺席人员
# Tuple[PersoneelInfos, AttendanceInfos]
# ->
# Tuple[Sequence[PersoneelInfo], Sequence[PersoneelInfo]]
partition_present_personeel_infos = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(1), partial(partial, swap_args(is_person_present))),
        itemgetter(0),
    ),
    starapply(partition),
    partial(map, tuple),
    tuple,
)

# 找出与人员匹配的参会信息
# Tuple[PersoneelInfo, AttendanceInfos]
# ->
# AttendanceInfo
match_attendance_info = pipe(
    dispatch(
        pipe(itemgetter(0), partial(partial, contains_formal_name)),
        itemgetter(1),
    ),
    starapply(filter),
    tuple,
    partial(ensure, pipe(len, partial(eq, 1))),
    itemgetter(0),
)

# 匹配出席人员和参会信息
# Tuple[PersoneelInfos, AttendanceInfos]
# ->
# Tuple[PersoneelAttendanceInfo, ...]
match_attendance_infos = pipe(
    tuple_args,
    dispatch(
        itemgetter(0),
        pipe(itemgetter(1), repeat),
    ),
    starapply(zip),
    partial(
        map,
        pipe(
            dispatch(
                itemgetter(0),
                match_attendance_info,
            ),
            tuple,
        ),
    ),
    tuple,
)

# 过滤未匹配的参会信息
# Tuple[PersoneelInfos, AttendanceInfos]
# ->
# AttendanceInfos
filter_unmatched_attendance_infos = pipe(
    tuple_args,
    dispatch(
        pipe(
            itemgetter(0),
            partial(partial, swap_args(is_attendance_matched)),
        ),
        itemgetter(1),
    ),
    starapply(filterfalse),
    tuple,
)


# 划分出席和缺席的人员和信息
# Tuple[PersoneelInfos, AttendanceInfos]
# ->
# Tuple[PersoneelAttendanceInfos, PersoneelInfos]
partition_present = pipe(
    tuple_args,
    dispatch(
        partition_present_personeel_infos,
        pipe(itemgetter(1), to_stream),
    ),
    chain.from_iterable,
    tuple,
    dispatch(
        pipe(
            dispatch(itemgetter(0), itemgetter(2)),
            starapply(match_attendance_infos),
        ),
        itemgetter(1),
    ),
    tuple,
)

# 参会时长是否充足
# Tuple[MeetingInfo, AttendanceInfo]
is_enough_attendance_time = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(1), attrgetter('attendance_time')),
        pipe(itemgetter(0), attrgetter('meeting_time')),
    ),
    starapply(ge),
)


# 划分时长足够与不足的出席人员
# Tuple[MeetingInfo, Sequence[PersoneelAttendanceInfo]]
# ->
# Tuple[Sequence[PersoneelAttendanceInfo], Sequence[PersoneelAttendanceInfo]]
partition_by_time = pipe(
    tuple_args,
    dispatch(
        pipe(
            dispatch(
                constant(itemgetter(1)),
                pipe(itemgetter(0), partial(partial, is_enough_attendance_time)),
            ),
            starapply(pipe),
        ),
        itemgetter(1),
    ),
    starapply(partition),
    partial(map, tuple),
    tuple,
)

# 划分信息
# Tuple[MeetingInfo, PersoneelInfos, AttendanceInfos]
# ->
# Tuple[PersoneelAttendanceInfos, PersoneelAttendanceInfos, PersoneelInfos]
partition_infos = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(0), to_stream),
        pipe(
            dispatch(itemgetter(1), itemgetter(2)),
            starapply(partition_present),
        ),
    ),
    chain.from_iterable,
    tuple,
    dispatch(
        pipe(
            dispatch(itemgetter(0), itemgetter(1)),
            starapply(partition_by_time),
        ),
        partial(islice_, start=2),
    ),
    chain.from_iterable,
    tuple,
)

# 获取团队列表
# PersoneelInfos -> Tuple[Tuple[str, str], ..]
get_group_teams_by_personeel_infos = pipe(
    partial(
        groupby,
        key=pipe(
            dispatch(
                attrgetter('group'),
                attrgetter('team'),
            ),
            tuple,
        ),
    ),
    partial(map, itemgetter(0)),
    tuple,
)

# 获取大组列表
# PersoneelInfos -> Tuple[str, ..]
get_groups_by_personeel_infos = pipe(
    partial(
        groupby,
        key=attrgetter('group'),
    ),
    partial(map, itemgetter(0)),
    tuple,
)

# str -> Callable[[PersoneelAttendanceInfo], bool]
# x -> pipe(
#     itemgetter(0), attrgetter('team'),
#     partial(eq, x)
# )
team_eq_for_personeel_attendance_info = pipe(
    dispatch(
        constant(itemgetter(0)),
        constant(attrgetter('team')),
        pipe(
            dispatch(
                constant(eq),
                identity,
            ),
            starapply(partial),
        ),
    ),
    starapply(pipe),
)

# str -> Callable[[PersoneelInfo], bool]
# x -> pipe(
#     attrgetter('team'),
#     partial(eq, x)
# )
team_eq_for_personeel_info = pipe(
    dispatch(
        constant(attrgetter('team')),
        pipe(
            dispatch(
                constant(eq),
                identity,
            ),
            starapply(partial),
        ),
    ),
    starapply(pipe),
)

TeamInfo = Tuple[PersoneelAttendanceInfos, PersoneelAttendanceInfos, PersoneelInfos]

# 过滤小组数据
# Tuple[Tuple[str, str], *TeamInfo]
# ->
# Tuple[Tuple[str, str], *TeamInfo]
filter_infos_by_group_team = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(0), to_stream),
        pipe(
            dispatch(
                pipe(
                    dispatch(
                        pipe(
                            itemgetter(0),
                            itemgetter(1),
                            team_eq_for_personeel_attendance_info,
                            partial(partial, filter),
                        ),
                        pipe(
                            itemgetter(0),
                            itemgetter(1),
                            team_eq_for_personeel_attendance_info,
                            partial(partial, filter),
                        ),
                        pipe(
                            itemgetter(0),
                            itemgetter(1),
                            team_eq_for_personeel_info,
                            partial(partial, filter),
                        ),
                    ),
                    starapply(cross),
                ),
                pipe(
                    partial(islice_, start=1),
                    tuple,
                ),
            ),
            starapply(invoke),
            partial(map, tuple),
            tuple,
        ),
    ),
    chain.from_iterable,
    tuple,
)

# 展开groupby
expand_groupby = pipe(
    partial(
        map,
        pipe(
            dispatch(
                itemgetter(0),
                pipe(itemgetter(1), tuple),
            ),
            tuple,
        ),
    ),
)

# 分组大组数据
# Tuple[Tuple[Tuple[str, str], *TeamInfo], ...]
# ->
# Dict[str, Tuple[Tuple[Tuple[str, str], *TeamInfo], ...]]
group_infos_by_group = pipe(
    tuple_args,
    partial(groupby, key=pipe(itemgetter(0), itemgetter(0))),
    expand_groupby,
    dict,
)

# 分组小组数据
# Tuple[Tuple[Tuple[str, str], *TeamInfo], ...]
# ->
# Dict[str, Tuple[Tuple[Tuple[str, str], *TeamInfo], ...]]
group_infos_by_team = pipe(
    tuple_args,
    partial(groupby, key=pipe(itemgetter(0), itemgetter(1))),
    partial(
        map,
        pipe(
            dispatch(
                itemgetter(0),
                pipe(
                    itemgetter(1), tuple,
                    partial(ensure, pipe(len, partial(eq, 1))),
                    itemgetter(0),
                ),
            ),
            tuple,
        ),
    ),
    dict,
)

# 组织信息
# Tuple[MeetingInfo, PersoneelInfos, AttendanceInfos]
# ->
# GroupAttendanceInfos
calc_group_attendance_infos = pipe(
    tuple_args,
    dispatch(
        pipe(
            itemgetter(1),
            get_group_teams_by_personeel_infos,
            to_stream,
        ),
        partition_infos,
    ),
    chain.from_iterable,
    tuple,
    pipe(
        partial(islice_, stop=4),
        tuple,
        dispatch(
            itemgetter(0),
            pipe(itemgetter(1), repeat),
            pipe(itemgetter(2), repeat),
            pipe(itemgetter(3), repeat),
        ),
        starapply(zip),
        partial(map, filter_infos_by_group_team),
        tuple,
        group_infos_by_group,
        methodcaller('items'),
        partial(
            map,
            pipe(
                dispatch(
                    itemgetter(0),
                    pipe(
                        itemgetter(1),
                        group_infos_by_team,
                    ),
                ),
            )
        ),
        dict,
    ),
)

# 小组出勤表转换为小组定位信息
# Worksheet -> Tuple[TeamLocation, ...]
group_attendance_sheet_to_team_locations = pipe(
    convert_group_attendance_sheet,
    partial(
        map,
        pipe(
            itemgetter(0), attrgetter('value'), str,
            if_(bool, TEAM_NAME_REGEX.match, constant(None)),
        ),
    ),
    partial(enumerate, start=1),
    partial(filter, itemgetter(1)),
    partial(
        map,
        pipe(
            dispatch(
                pipe(itemgetter(1), methodcaller('group', 1)),
                itemgetter(0),
            ),
            starapply(TeamLocation),
        ),
    ),
    tuple,
)

# 生成出席指令
# Tuple[int, int, PersoneelAttendanceInfo, bool]
# ->
# $[FillCommand, ...]
generate_present_command = pipe(
    dispatch(
        pipe(
            dispatch(
                itemgetter(0),
                constant(2),
                itemgetter(1),
                constant(False)
            ),
            tuple,
        ),
        pipe(
            dispatch(
                itemgetter(0),
                constant(3),
                pipe(
                    itemgetter(2), itemgetter(1),
                    attrgetter('origin_name'), get_nickname
                ),
                constant(False)
            ),
            tuple,
        ),
        pipe(
            dispatch(
                itemgetter(0),
                constant(4),
                pipe(
                    itemgetter(2), itemgetter(1), attrgetter('attendance_time'),
                    methodcaller('strftime', '%H:%M:%S'),
                ),
                itemgetter(3)
            ),
            tuple,
        ),
    ),
)

# 生成出席指令
# Tuple[TeamLocation, Tuple[PersoneelAttendanceInfo, ...], Tuple[bool, ...]]
# ->
# Tuple[Tuple[int, int, str, str, bool], ...]
generate_present_commands = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(0), itemgetter(1), partial(add, 1), count),
        pipe(constant(1), count),
        itemgetter(1),
        itemgetter(2),
    ),
    starapply(zip),
    # $Tuple[int, int, PersoneelAttendanceInfo, bool]
    partial(map, generate_present_command),
    chain.from_iterable,
    tuple,
)

# 合并出席信息
# Tuple[PersoneelAttendanceInfos, PersoneelAttendanceInfos]
# ->
# PersoneelAttendanceInfos
merge_present_infos = pipe(
    tuple_args,
    chain.from_iterable,
    partial(
        sorted,
        key=pipe(itemgetter(0), attrgetter('number'))
    ),
    tuple,
)

# 计算出席时长不足标记
# Tuple[PersoneelAttendanceInfos, PersoneelAttendanceInfos]
# ->
# Tuple[bool, ...]
calc_present_leak_of_time_flags =pipe(
    tuple_args,
    partial(
        map,
        pipe(partial(map, itemgetter(0)), tuple)
    ),
    tuple,
    # Tuple[PersoneelInfos, PersoneelInfos]
    dispatch(
        itemgetter(0),
        pipe(itemgetter(1), repeat),
    ),
    starapply(zip),
    partial(starmap, swap_args(contains)),
    tuple,
)


# 合并缺席和时长不足人员信息。
# Tuple[PersoneelAttendanceInfos, PersoneelInfos] -> PersoneelInfos
merge_absent_and_leak_infos = pipe(
    tuple_args,
    cross(
        partial(map, itemgetter(0)),
        identity,
    ),
    chain.from_iterable,
    partial(sorted, key=attrgetter('number')),
    tuple,
)


# 生成缺席指令
# Tuple[int, PersoneelInfo] -> Tuple[int, int, str, str, bool]
generate_absent_command = pipe(
    dispatch(
        itemgetter(0),
        constant(1),
        pipe(itemgetter(1), attrgetter('formal_name')),
        constant(False),
    ),
    tuple,
)

# 生成缺席指令
# Tuple[TeamLocation, PersoneelInfos]
# ->
# Tuple[Tuple[int, int, str, str, bool], ...]
generate_absent_commands = pipe(
    tuple_args,
    dispatch(
        pipe(
            cross(
                pipe(itemgetter(1), partial(add, 1), count),
                identity,
            ),
            starapply(zip),
            # $Tuple[int, PersoneelInfo]
            partial(map, generate_absent_command),
            tuple,
        ),
        pipe(
            itemgetter(0),
            itemgetter(1),
            partial(add, 1),
            dispatch(
                identity,
                constant(1),
                constant('全勤'),
                constant(False),
            ),
            tuple,
            to_stream,
        ),
    ),
    tuple,
    if_(itemgetter(0), itemgetter(0), itemgetter(1)),
)

# 生成标题指令
# Tuple[TeamLocation, TeamAttendanceInfo] -> FillCommand
generate_title_command = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(0), itemgetter(1)),
        constant(1),
        pipe(
            dispatch(
                pipe(
                    itemgetter(0),
                    itemgetter(0),
                    '{0}组（{{0}}人）'.format,
                    attrgetter('format'),  # str.format
                ),
                pipe(
                    itemgetter(1),
                    partial(islice_, start=1),
                    chain.from_iterable,
                    partial(map, constant(1)),
                    sum,
                ),
            ),
            starapply(invoke),
        ),
        constant(False),
    ),
    tuple,
)

# input:
# team_location: TeamLocation
# group_attendance_info: GroupAttendanceInfo
GRAPH_TEAM_COMMANDS = make_graph(
    (
        ('team', 'lineno'), 'team_location', identity
    ),
    (
        'team_attendance_info', ('team', 'group_attendance_info'), item_extract
    ),
    (
        ('team_info', 'enough_of_time_infos', 'leak_of_time_infos', 'absent_infos'),
        'team_attendance_info', identity
    ),
    (
        'present_infos', ('enough_of_time_infos', 'leak_of_time_infos'),
        merge_present_infos
    ),
    (
        'leak_of_time_present_flags', ('present_infos', 'leak_of_time_infos'),
        calc_present_leak_of_time_flags,
    ),
    (
        'title_command', ('team_location', 'team_attendance_info'),
        generate_title_command
    ),
    (
        'title_commands', 'title_command', to_stream
    ),
    (
        'present_commands',
        ('team_location', 'present_infos', 'leak_of_time_present_flags'),
        generate_present_commands
    ),
    (
        'absent_and_leak_infos', ('leak_of_time_infos', 'absent_infos'),
        merge_absent_and_leak_infos,
    ),
    (
        'absent_commands', ('team_location', 'absent_and_leak_infos'),
        generate_absent_commands
    ),
    (
        'present_absent_commands',
        ('title_commands', 'present_commands', 'absent_commands'),
        pipe(chain.from_iterable, tuple),
    ),
)

# 生成小组指令
# Tuple[TeamLocation, GroupAttendanceInfo]
# ->
# Tuple[Tuple[int, str, str, bool], ...]
generate_team_commands = pipe(
    tuple_args,
    dispatch(
        pipe(
            dispatch(
                constant('team_location'),
                itemgetter(0),
            ),
            tuple,
        ),
        pipe(
            dispatch(
                constant('group_attendance_info'),
                itemgetter(1),
            ),
            tuple,
        ),
    ),
    tuple,
    partial(eval_graph, GRAPH_TEAM_COMMANDS, 'present_absent_commands'),
)

# 根据小组定位与信息表生成小组表填充指令
# Tuple[Tuple[TeamLocation, ...], GroupAttendanceInfo]
# ->
# Tuple[FillCommand, ...]
generate_team_commands_by_locations_and_group_info = pipe(
    tuple_args,
    cross(identity, repeat),
    starapply(zip),
    # $[Tuple[TeamLocation, TeamAttendanceInfos], ...]
    partial(map, generate_team_commands),
    # $[Tuple[Tuple[int, str, bool], ...]]
    chain.from_iterable,
    tuple,
)

# 生成没有匹配的出席信息表填充指令
# Tuple[int, AttendanceInfo] -> $[FillCommand, ...]
generate_mismatched_command = pipe(
    dispatch(
        pipe(
            dispatch(
                itemgetter(0),
                constant(1),
                pipe(itemgetter(1), attrgetter('origin_name')),
                constant(False),
            ),
            tuple,
        ),
        pipe(
            dispatch(
                itemgetter(0),
                constant(2),
                pipe(
                    itemgetter(1),
                    attrgetter('attendance_time'),
                    methodcaller('strftime', '%H:%M:%S'),
                ),
                constant(False),
            ),
            tuple,
        ),
    ),
)

# 生成没有匹配的出席信息表填充指令
# AttendanceInfos -> Tuple[FillCommand, ...]
generate_mismatched_commands = pipe(
    partial(enumerate, start=2),
    partial(map, generate_mismatched_command),
    chain.from_iterable,
    tuple,
)

# 添加团队定位尾部
# Tuple[TeamLocation, ...] -> $[TeamLocation, ...]
add_team_locations_tail = pipe(
    dispatch(
        identity,
        pipe(
            itemgetter(-1),
            itemgetter(1),
            partial(add, 10),
            dispatch(
                constant('尾部'),
                identity,
            ),
            starapply(TeamLocation),
            to_stream,
        ),
    ),
    chain.from_iterable,
)

generate_clean_commands_by_lineno = dispatch(
    pipe(
        dispatch(
            identity,
            constant(1),
            constant(''),
            constant(False),
        ),
        create_fill_command,
    ),
    pipe(
        dispatch(
            identity,
            constant(2),
            constant(''),
            constant(False),
        ),
        create_fill_command,
    ),
    pipe(
        dispatch(
            identity,
            constant(3),
            constant(''),
            constant(False),
        ),
        create_fill_command,
    ),
    pipe(
        dispatch(
            identity,
            constant(4),
            constant(''),
            constant(False),
        ),
        create_fill_command,
    ),
)

# 生成表格清理指令
# $[TeamLocation, ...] -> Tuple[FillCommand, ...]
generate_clean_commands = pipe(
    pairwise,
    partial(
        map,
        pipe(
            cross(
                pipe(itemgetter(1), partial(add, 1)),
                itemgetter(1),
            ),
            starapply(range),
            partial(map, generate_clean_commands_by_lineno),
            chain.from_iterable,
        )
    ),
    chain.from_iterable,
    tuple,
)

# 填充单元格
# Tuple[WorksheetCell, FillCommand] -> None
fill_workcell = pipe(
    tuple_args,
    side_effect(
        pipe(
            dispatch(
                itemgetter(0),
                constant('value'),
                pipe(itemgetter(1), itemgetter(2)),
            ),
            starapply(setattr),
        ),
    ),
    if_(
        pipe(itemgetter(1), itemgetter(3)),
        pipe(
            dispatch(
                itemgetter(0),
                constant('font'),
                constant(RED_FONT),
            ),
            starapply(setattr),
        ),
        pipe(
            dispatch(
                itemgetter(0),
                constant('font'),
                constant(BLACK_FONT),
            ),
            starapply(setattr),
        )
    ),
)

# 填充工作表
# Tuple[Worksheet, FillCommand] -> None
fill_worksheet_command = pipe(
    tuple_args,
    dispatch(
        pipe(
            dispatch(
                pipe(itemgetter(0), attrgetter('cell')),
                pipe(itemgetter(1), itemgetter(0)),
                pipe(itemgetter(1), itemgetter(1)),
            ),
            starapply(invoke),
        ),
        itemgetter(1),
    ),
    tuple,
    fill_workcell,
)

# 填充工作表
# Tuple[Worksheet, Tuple[FillCommand, ...]] -> Tuple[FillCommand, ...]
fill_worksheet_commands = pipe(
    tuple_args,
    side_effect(
        pipe(
            cross(repeat, identity),
            starapply(zip),
            partial(map, fill_worksheet_command),
            tuple,
        ),
    ),
    itemgetter(1),
)

# inputs:
# group_name: str
# group_attendance_infos: GroupAttendanceInfos
# summary_workbook: Workbook
GRAPH_GROUP_SHEET = make_graph(
    (
        'group_attendance_info', ('group_name', 'group_attendance_infos'),
        item_extract
    ),
    (
        'group_attendance_sheet', ('group_name', 'summary_workbook'),
        item_extract
    ),
    (
        'team_locations', 'group_attendance_sheet',
        group_attendance_sheet_to_team_locations
    ),
    (
        'team_locations_with_tail', 'team_locations', add_team_locations_tail
    ),
    (
        'clean_commands', 'team_locations_with_tail', generate_clean_commands
    ),
    (
        'team_commands', ('team_locations', 'group_attendance_info'),
        generate_team_commands_by_locations_and_group_info
    ),
    (
        'fill_clean_commands', ('group_attendance_sheet', 'clean_commands'),
        fill_worksheet_commands
    ),
    (
        'fill_team_commands', ('group_attendance_sheet', 'team_commands'),
        fill_worksheet_commands
    ),
    (
        'fill_commands', Chain(('fill_clean_commands', 'fill_team_commands')),
        identity
    ),
)

# 填充各大组命令
fill_groups_commands = pipe(
    tuple_args,
    cross(identity, repeat, repeat),
    starapply(zip),
    partial(
        map,
        pipe(
            partial(
                zip_refs_values,
                ('group_name', 'group_attendance_infos', 'summary_workbook')
            ),
            partial(
                eval_graph, GRAPH_GROUP_SHEET,
                ('group_name', 'fill_commands'),
            ),
        ),
    ),
    tuple,
)

# inputs:
# args: Namespace
GRAPH_MAIN = make_graph(
    ('meeting_path', 'args', attrgetter('meeting')),
    ('debug_flag', 'args', attrgetter('debug')),
    (
        'summary_workbook_filepath', 'meeting_path',
        pipe(
            dispatch(identity, constant(MEETING_SUMMARY_FILENAME)),
            starapply(os.path.join),
        )
    ),
    (
        'summary_workbook_output_filepath', 'meeting_path',
        pipe(
            dispatch(identity, constant(MEETING_SUMMARY_OUTPUT_FILENAME)),
            starapply(os.path.join),
        )
    ),
    (
        'fill_output_filepath', 'meeting_path',
        pipe(
            dispatch(identity, constant('fill_commands.txt')),
            starapply(os.path.join),
        )
    ),
    (
        'attendance_workbook_filepath', 'meeting_path',
        pipe(
            dispatch(identity, constant(MEETING_ATTENDANCE_FILENAME)),
            starapply(os.path.join),
        )
    ),
    ('summary_workbook', 'summary_workbook_filepath', load_workbook),
    ('attendance_workbook', 'attendance_workbook_filepath', load_workbook),
    ('people_sheet', 'summary_workbook', itemgetter(PEOPLE_SHEET_NAME)),
    ('meeting_info_sheet', 'summary_workbook', itemgetter(MEETING_INFO_SHEET_NAME)),
    ('meeting_info', 'meeting_info_sheet', parse_meeting_info_sheet),
    ('personeel_infos', 'people_sheet', parse_people_sheet),
    (
        'attendance_infos', 'attendance_workbook',
        pipe(
            itemgetter(DETAIL_OF_MEMBER_ATTENDANCE),
            parse_attendance_detail_sheet
        )
    ),
    (
        'mismatched_attendance_infos', ('personeel_infos', 'attendance_infos'),
        filter_unmatched_attendance_infos
    ),
    (
        'group_attendance_infos',
        ('meeting_info', 'personeel_infos', 'attendance_infos'),
        calc_group_attendance_infos
    ),
    ('group_names', 'personeel_infos', get_groups_by_personeel_infos),
    (
        'fill_groups_commands',
        ('group_names', 'group_attendance_infos', 'summary_workbook'),
        fill_groups_commands
    ),
    (
        'mismatched_commands', 'mismatched_attendance_infos',
        generate_mismatched_commands
    ),
    (
        'mismatched_sheet', 'summary_workbook',
        partial(item_extract, MISMATCHED_SHEET_NAME)
    ),
    (
        'fill_mismatched_commands', ('mismatched_sheet', 'mismatched_commands'),
        fill_worksheet_commands
    ),
    (
        'save',
        (
            'summary_workbook', 'summary_workbook_output_filepath',
            'debug_flag', 'fill_output_filepath',
            'fill_groups_commands', 'fill_mismatched_commands'
        ),
        pipe(
            side_effect(
                pipe(
                    dispatch(
                        pipe(itemgetter(0), attrgetter('save')),
                        itemgetter(1),
                    ),
                    starapply(invoke),
                )
            ),
            side_effect(
                pipe(
                    itemgetter(1),
                    "保存'{0}'文件成功。".format,
                    print,
                )
            ),
            side_effect(
                if_(
                    itemgetter(2),
                    pipe(
                        dispatch(
                            itemgetter(3),
                            pipe(
                                dispatch(
                                    itemgetter(4), itemgetter(5),
                                ),
                                tuple,
                                pformat,
                            ),
                        ),
                        starapply(save_file),
                        # TODO: 打屏“保存调试信息成功。”
                    )
                ),
            ),
        )
    ),
)

main_process = pipe(
    dispatch(
        constant(('args',)),
        to_stream,
    ),
    starapply(zip),
    tuple,
    partial(eval_graph, GRAPH_MAIN, 'save'),
)
