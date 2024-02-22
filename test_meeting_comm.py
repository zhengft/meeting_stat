#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from functools import partial
from operator import add, itemgetter

import pytest

from meeting_comm import (
    GraphRule, MissingTarget, assign_outputs, calc_execute_rules,
    dispatch, eval_graph, eval_graph_rule, eval_refs,
    identity, make_graph, pipe, starapply, target_matched, tuple_args,
    zip_refs_values
)


TEST_GRAPH_RULE_01 = GraphRule('output', 'input', partial(add, 1))
TEST_GRAPH_RULE_02 = GraphRule('output', ('input0', 'input1'), starapply(add))
TEST_GRAPH_RULE_03 = GraphRule(
    ('output0', 'output1'), ('input0', 'input1', 'input2'), pipe(
        tuple_args,
        dispatch(
            pipe(
                dispatch(itemgetter(0), itemgetter(1)),
                starapply(add),
            ),
            pipe(
                dispatch(itemgetter(1), itemgetter(2)),
                starapply(add),
            ),
        ),
        tuple,
    )
)

TEST_GRAPH_01 = make_graph(
    TEST_GRAPH_RULE_01
)

TEST_GRAPH_RULE_02_01 = ('target1', 'input1', identity)
TEST_GRAPH_RULE_02_02 = ('target2', 'target1', identity)
TEST_GRAPH_RULE_02_03 = (
    'final', ('input0', 'target1', 'target2'), pipe(tuple_args, identity)
)

TEST_GRAPH_02 = make_graph(
    TEST_GRAPH_RULE_02_01,
    TEST_GRAPH_RULE_02_02,
    TEST_GRAPH_RULE_02_03,
)

TEST_DATA_01 = {
    'input0': 0,
    'input1': 1,
    'input2': 2,
}


def test_make_graph_01():
    result = make_graph()
    assert result is not None


def test_target_matched_01():
    result = target_matched('aaa', 'aaa')
    assert result is True


def test_target_matched_02():
    result = target_matched('aaa', 'bbb')
    assert result is False


def test_target_matched_03():
    result = target_matched('aaa', ('aaa', 'bbb'))
    assert result is True


def test_target_matched_04():
    result = target_matched('aaa', ('ccc', 'bbb'))
    assert result is False


def test_calc_execute_rules_01():
    result = calc_execute_rules(('output',), TEST_GRAPH_01, {'input', 1})
    expected = (TEST_GRAPH_RULE_01,)
    assert expected == result


def test_calc_execute_rules_02():
    with pytest.raises(MissingTarget) as ex:
        calc_execute_rules(('not_exists',), TEST_GRAPH_01, {})
    assert 'not_exists' == str(ex.value)


def test_calc_execute_rules_03():
    with pytest.raises(MissingTarget) as ex:
        calc_execute_rules(('output',), TEST_GRAPH_01, {})
    assert 'input' == str(ex.value)


def test_eval_refs_01():
    result = eval_refs('input0', TEST_DATA_01)
    assert 0 == result


def test_eval_refs_02():
    result = eval_refs('input1', TEST_DATA_01)
    assert 1 == result


def test_eval_refs_03():
    result = eval_refs(('input0',), TEST_DATA_01)
    assert (0,) == result


def test_eval_refs_04():
    result = eval_refs(('input0', 'input1'), TEST_DATA_01)
    assert (0, 1) == result


def test_eval_refs_05():
    result = eval_refs(('input0', ('input1', 'input2')), TEST_DATA_01)
    assert (0, (1, 2)) == result


def test_eval_refs_06():
    result = eval_refs((('input0', 'input1'), 'input2'), TEST_DATA_01)
    assert ((0, 1), 2) == result


def test_eval_graph_rule_01():
    result = eval_graph_rule(TEST_GRAPH_RULE_01, {'input': 1})
    assert 2 == result


def test_eval_graph_rule_02():
    result = eval_graph_rule(TEST_GRAPH_RULE_01, {'input': 2})
    assert 3 == result


def test_eval_graph_rule_03():
    result = eval_graph_rule(TEST_GRAPH_RULE_02, {'input0': 1, 'input1': 1})
    assert 2 == result


def test_eval_graph_rule_04():
    result = eval_graph_rule(TEST_GRAPH_RULE_02, {'input0': 1, 'input1': 2})
    assert 3 == result


def test_eval_graph_rule_05():
    result = eval_graph_rule(
        TEST_GRAPH_RULE_03, {'input0': 1, 'input1': 2, 'input2': 3}
    )
    assert (3, 5) == result


def test_zip_refs_values_01():
    result = tuple(zip_refs_values('output', 2))
    expected = (('output', 2),)
    assert expected == result


def test_zip_refs_values_02():
    result = tuple(zip_refs_values(('output0', 'output1'), (3, 5)))
    expected = (('output0', 3), ('output1', 5))
    assert expected == result


def test_assign_outputs_01():
    data = {}
    assign_outputs(zip_refs_values('output', 2), data)
    expected = {'output': 2}
    assert expected == data


def test_assign_outputs_02():
    data = {}
    assign_outputs(zip_refs_values(('output0', 'output1'), (3, 5)), data)
    expected = {'output0': 3, 'output1': 5}
    assert expected == data


def test_eval_graph_01():
    # result = eval_graph(TEST_GRAPH_01, 'output', (('input', 1),))
    expected = ('output', 2)
    # assert expected == result


def test_eval_graph_02():
    result = eval_graph(
        TEST_GRAPH_02, 'final', (('input0', 0), ('input1', 1))
    )
    expected = (0, 1, 1)
    assert expected == result
