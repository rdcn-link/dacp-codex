import json
import pytest
from dacp.variables import resolve_variables


def test_typed_values_nested_and_nonrecursive():
    text = 'tasks: [{params: {n: $N, nested: [$B, $O, $A, $NULL], text: $TEXT, literal: "x$N"}}]'
    values = dict(N=123, B=True, O={'x': 2}, A=[1, 2], NULL=None, TEXT='$N')
    result = json.loads(resolve_variables(text, values))['tasks'][0]['params']
    assert result == dict(n=123, nested=[True, {'x': 2}, [1, 2], None], text='$N', literal='x$N')
    assert values['TEXT'] == '$N'


def test_missing_variable_fails():
    with pytest.raises(ValueError, match='missing workflow variable: N'):
        resolve_variables('params: {n: $N}', {})


@pytest.mark.parametrize('values', [[], {'bad-name': 1}, {'N': float('nan')}])
def test_reject_invalid_variables(values):
    with pytest.raises(ValueError):
        resolve_variables('params: {}', values)


def test_no_variables_preserves_original_and_escaping():
    assert resolve_variables('params: {n: $N}') == 'params: {n: $N}'
    assert json.loads(resolve_variables('params: {"$N": "$(touch x)"}', {})) == {'params': {'$N': '$(touch x)'}}


def test_yaml_injection_remains_data():
    result = resolve_variables('params: {text: $TEXT}', {'TEXT': 'x\ntasks: []\n!!python/object:foo'})
    assert json.loads(result)['params']['text'].startswith('x\ntasks: []')


def test_recursive_yaml_rejected():
    with pytest.raises(ValueError, match='nesting'):
        resolve_variables('x: &x [*x]', {})
