import pytest
from dacp import yaml_document


def test_typed_yaml_roundtrip():
    value = yaml_document.mapping("count: 123\nratio: 1.25\nenabled: true\ncode: '001'\nlabel: 'true'\nitems: [1, null, '2026-10-27']\n")
    assert yaml_document.mapping(yaml_document.dump(value)) == value
    assert value['count'] == 123 and value['code'] == '001'


@pytest.mark.parametrize('text', ['x: 1\nx: 2', 'x: &x [1]\ny: *x', 'x: .nan', '1: value'])
def test_reject_ambiguous_or_unbounded_yaml(text):
    with pytest.raises(ValueError):
        yaml_document.load(text)
