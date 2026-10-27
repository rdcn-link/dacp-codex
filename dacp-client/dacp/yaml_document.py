"""Data-only YAML documents for workflows, policies and variables."""
import math
import yaml
from yaml.events import AliasEvent

class _Loader(yaml.SafeLoader):
    def compose_node(self, parent, index):
        if self.check_event(AliasEvent):
            raise ValueError('YAML aliases are not supported')
        self._depth = getattr(self, '_depth', 0) + 1
        if self._depth > 100:
            raise ValueError('YAML nesting exceeds 100 levels')
        try:
            return super().compose_node(parent, index)
        finally:
            self._depth -= 1

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                raise ValueError('YAML mapping keys must be strings')
            if key in result:
                raise ValueError('duplicate YAML mapping key: ' + key)
            result[key] = self.construct_object(value_node, deep=deep)
        return result

def _check(value, depth=0):
    if depth > 100:
        raise ValueError('YAML nesting exceeds 100 levels')
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError('YAML mapping keys must be strings')
            _check(v, depth+1)
    elif isinstance(value, list):
        for v in value:
            _check(v, depth+1)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError('YAML numbers must be finite')
    elif value is not None and not isinstance(value, (str, int, bool)):
        raise ValueError('unsupported YAML value; quote dates and other textual values')

def load(text):
    if len(text.encode('utf-8')) > 1024 * 1024:
        raise ValueError('YAML document exceeds 1 MiB')
    value = yaml.load(text, Loader=_Loader)
    _check(value)
    return value

def mapping(text):
    value = load(text)
    if not isinstance(value, dict):
        raise ValueError('YAML document must be a mapping')
    return value

def dump(value):
    _check(value)
    return yaml.safe_dump(value, allow_unicode=True, sort_keys=False)
