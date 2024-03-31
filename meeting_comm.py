#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from collections import deque
from functools import partial
from itertools import chain, filterfalse, groupby, islice, tee
from operator import contains, eq, itemgetter, not_
from pathlib import Path
from typing import (
    Any, Callable, Iterator, NamedTuple, Union, Tuple, TypeVar,
)


MEETING_SUMMARY_FILENAME = '生活修行考勤表.xlsx'
MEETING_SUMMARY_OUTPUT_FILENAME = '生活修行考勤表（生成）.xlsx'
MEETING_ATTENDANCE_FILENAME = '考勤数据.xlsx'

SUFFIX_NUMBER = re.compile(r'\d+$')


class StatError(Exception):
    """统计异常。"""


class MissingTarget(StatError):
    """缺失目标。"""


class EvalGraphRuleError(StatError):
    """规则求值错误。"""


class InvalidAttendanceInfo(StatError):
    """无效的参会信息。"""


class Cell(NamedTuple):
    """单元格。"""
    value: Union[str, int, None]


A = TypeVar('A')
B = TypeVar('B')


class Chain(tuple):
    """顺序调用链。"""


Targets = Union[str, Tuple[str, ...], Chain]


class GraphRule(NamedTuple):
    outputs: Targets
    inputs: Targets
    action: Callable

Graph = Tuple[GraphRule, ...]

def constant(x: A) -> Callable:
    """常量。"""
    def constant_func(*args, **kwargs) -> A:
        return x
    return constant_func


def cross(*funcs) -> Callable:
    """交错。"""
    def cross_func(x):
        return map(starapply(invoke), zip(funcs, x))
    return cross_func


def debug(x: A) -> A:
    """调试。"""
    breakpoint()
    return x


def dispatch(*funcs) -> Callable:
    """分派。"""
    def dispatch_func(*args, **kwargs):
        return (func(*args, **kwargs) for func in funcs)
    return dispatch_func


def ensure(predicate, x):
    """确认。"""
    assert predicate(x)
    return x


def identity(x: A) -> A:
    """同一。"""
    return x


def if_(predicate: Callable,
        then_func: Callable,
        else_func: Callable = None) -> Callable:
    def if_func(x):
        if predicate(x):
            return then_func(x)
        if else_func:
            return else_func(x)
        return x
    return if_func


def invoke(func: Callable, *args, **kwargs):
    """调用。"""
    return func(*args, **kwargs)


def islice_(iterable, start = None, stop = None, step = None):
    """切片。"""
    return islice(iterable, start, stop, step)


def make_graph(*args: Tuple[str, str, Callable]):
    """创建图。"""
    return tuple(GraphRule(*arg) for arg in args)


def partition(pred, iterable):
    """Partition entries into true entries and false entries.

    If *pred* is slow, consider wrapping it with functools.lru_cache().
    """
    # partition(is_even, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return filter(pred, t1), filterfalse(pred, t2)


def pipe(*funcs):
    """函数管道。"""
    def pipe_func(*args, **kwargs):
        result = funcs[0](*args, **kwargs)
        for func in funcs[1:]:
            result = func(result)
        return result
    return pipe_func


def raise_(exp: Exception):
    raise exp


def side_effect(func):
    def side_effect_func(x):
        func(x)
        return x
    return side_effect_func


def starapply(func: Callable) -> Callable:
    """展开参数并应用。"""
    def starapply_func(x):
        if isinstance(x, dict):
            return func(**x)
        return func(*x)
    return starapply_func


def swap_args(func: Callable) -> Callable:
    """交换函数的前2个参数。"""
    def swap_args_func(*args, **kwargs):
        return func(args[1], args[0], *args[2:], **kwargs)
    return swap_args_func


def to_stream(value: A) -> Iterator[A]:
    """转换为流。"""
    yield value


def create_tuple(*args) -> Tuple:
    """创建元组。"""
    return tuple(args)


def tuple_args(*args) -> Tuple:
    """入参转换为元组。用于标准化多参函数的入参。"""
    if len(args) == 1:
        return args[0]
    return tuple(args)


def unique_justseen(iterable, key=None):
    "List unique elements, preserving order. Remember only the element just seen."
    # unique_justseen('AAAABBBCCDAABBB') --> A B C D A B
    # unique_justseen('ABBcCAD', str.lower) --> A B c A D
    return map(next, map(itemgetter(1), groupby(iterable, key)))


##########  ##########

# 目标转换为目标序列
target_to_targets = pipe(
    if_(
        partial(swap_args(isinstance), tuple),
        iter,
        to_stream,
    )
)


# 目标是否匹配规则
# Tuple[str, Union[str, Tuple[str, ...]]] -> bool
target_matched = pipe(
    tuple_args,
    dispatch(
        pipe(itemgetter(1), target_to_targets),
        itemgetter(0),
    ),
    starapply(contains),
)


def calc_execute_rules(goals: Tuple[str, ...],
                       graph: Graph,
                       data: dict) -> Tuple[GraphRule, ...]:
    """计算执行规则序列。"""
    targets = deque(goals)
    visited = set()

    def dfs(target: str) -> Iterator[GraphRule]:
        if target in data:
            return
        try:
            matched_rule = next(
                filter(
                    pipe(itemgetter(0), partial(target_matched, target)), graph
                )
            )
        except StopIteration:
            raise MissingTarget(target)

        if matched_rule not in visited:
            visited.add(matched_rule)
            need_targets = tuple(
                filter(
                    pipe(partial(contains, data), not_),
                    target_to_targets(matched_rule[1])
                )
            )
            yield from chain.from_iterable(map(dfs, need_targets))
            yield matched_rule

    return tuple(chain.from_iterable(map(dfs, targets)))


def eval_refs(refs: Union[str, Tuple[str, ...]],
              data: dict) -> Union[Any, Tuple[Any, ...]]:
    """引用求值。"""
    if isinstance(refs, tuple):
        return tuple(eval_refs(ref, data) for ref in refs)
    return data[refs]


def eval_graph_rule(rule: GraphRule, data: dict) -> Union[Any, Tuple[Any, ...]]:
    """规则求值。"""
    inputs = eval_refs(rule.inputs, data)
    outputs = rule.action(inputs)
    return outputs


def zip_refs_values(refs: Union[str, Tuple[str, ...]],
                    values: Union[Any, Tuple[Any, ...]]
                    ) -> Iterator[Tuple[str, Any]]:
    """匹配输出引用和输出。"""
    if isinstance(refs, tuple):
        assert len(refs) == len(values), f'assert length of {refs} and {values}'
        for ref, value in zip(refs, values):
            yield from zip_refs_values(ref, value)
    else:
        yield refs, values


def assign_outputs(outputs: Iterator[Tuple[str, Any]], data: dict):
    """输出结果赋值到data中。

    TODO: 处理嵌套结构赋值。
    """
    data.update(dict(outputs))


class _Targets(NamedTuple):
    """目标序列。"""
    results: Tuple[str]
    depends_only: Tuple[str]

    @property
    def depend_targets(self) -> Tuple[str]:
        return tuple(
            chain(
                tuple(target_to_targets(self.results)),
                self.depends_only
            )
        )

    @property
    def result_targets(self) -> Tuple[str]:
        return self.results


def create_targets(results: Union[str, Tuple[str, ...]],
                   depends_only: Union[str, Tuple[str, ...]] = tuple()
                   ) -> _Targets:
    """创建目标序列。"""
    depends_only = tuple(target_to_targets(depends_only))
    return _Targets(results, depends_only)


def eval_graph(graph: Graph,
               goal: Union[str, Tuple[str, ...]],
               pairs) -> Callable:
    """图求值。"""
    data = {pair[0]: pair[1] for pair in pairs}
    if isinstance(goal, _Targets):
        targets = goal
    else:
        targets = create_targets(goal)
    execute_rules = calc_execute_rules(targets.depend_targets, graph, data)
    for rule in execute_rules:
        try:
            outputs = eval_graph_rule(rule, data)
        except Exception as ex:
            raise EvalGraphRuleError(rule) from ex
        assign_outputs(zip_refs_values(rule.outputs, outputs), data)
    return eval_refs(targets.result_targets, data)


##########  ##########

def save_file(filepath: Union[str, Path], content: str):
    """保存文件。"""
    with open(str(filepath), 'w', encoding='utf-8') as file:
        file.write(content)
