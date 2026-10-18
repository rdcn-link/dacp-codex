"""Typed, transport-independent records predicates. No SQL text is accepted."""
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
import math
from . import dacp_pb2 as pb


def _literal(value):
    if isinstance(value, bool): kind, text = 'boolean', str(value).lower()
    elif isinstance(value, int): kind, text = 'integer', str(value)
    elif isinstance(value, float):
        if not math.isfinite(value): raise ValueError('floating literal must be finite')
        kind, text = 'float', repr(value)
    elif isinstance(value, Decimal):
        if not value.is_finite(): raise ValueError('decimal literal must be finite')
        kind, text = 'decimal', str(value)
    elif isinstance(value, str): kind, text = 'string', value
    elif isinstance(value, datetime):
        if value.utcoffset() is None: kind, text = 'datetime', value.isoformat()
        else: kind, text = 'timestamp', value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    elif isinstance(value, date): kind, text = 'date', value.isoformat()
    else: raise ValueError('unsupported literal; use is_null() for nulls')
    return pb.Literal(type=kind, value=text)


@dataclass(frozen=True)
class Predicate:
    _message: object
    def __and__(self, other): return self._combine('and', other)
    def __or__(self, other): return self._combine('or', other)
    def __invert__(self): return Predicate(pb.Predicate(op='not', children=[self._message]))
    def __bool__(self): raise TypeError('combine predicates using &, |, and ~, with parentheses')
    def _combine(self, op, other):
        if not isinstance(other, Predicate): raise TypeError('expected a predicate')
        return Predicate(pb.Predicate(op=op, children=[self._message, other._message]))


class Column:
    def __init__(self, name):
        if not isinstance(name, str) or not name: raise ValueError('expected column name')
        self.name = name
    def _compare(self, op, value):
        return Predicate(pb.Predicate(op=op, column=self.name, values=[_literal(value)]))
    def __eq__(self, value): return self._compare('eq', value)
    def __ne__(self, value): return self._compare('ne', value)
    def __lt__(self, value): return self._compare('lt', value)
    def __le__(self, value): return self._compare('le', value)
    def __gt__(self, value): return self._compare('gt', value)
    def __ge__(self, value): return self._compare('ge', value)
    def isin(self, values):
        return Predicate(pb.Predicate(op='in', column=self.name, values=[_literal(v) for v in values]))
    def is_null(self): return Predicate(pb.Predicate(op='is_null', column=self.name))
    def is_not_null(self): return Predicate(pb.Predicate(op='is_not_null', column=self.name))


def col(name): return Column(name)


def scan_options(predicate=None, columns=None, limit=None):
    if predicate is None and columns is None and limit is None: return None
    scan = pb.Scan()
    if predicate is not None:
        if not isinstance(predicate, Predicate): raise TypeError('expected a predicate')
        scan.predicate.CopyFrom(predicate._message)
    if columns is not None:
        if isinstance(columns, (str, bytes)): raise ValueError("columns must be a sequence of names")
        columns = list(columns)
        if (not columns or any(not isinstance(c, str) or not c for c in columns)
                or len(set(columns)) != len(columns)):
            raise ValueError('projection requires unique nonempty column names')
        scan.projection.columns.extend(columns)
    if limit is not None:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 0 <= limit <= 2**63-1:
            raise ValueError('limit must be a nonnegative int64')
        scan.limit = limit
    return scan
