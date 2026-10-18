"""Translate a deliberately small SQL subset into public DACP scan expressions."""
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import math

import pyarrow as pa
import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.tokens import TokenType

from .expressions import Predicate, _literal
from .source import _logical_path
from . import dacp_pb2 as pb


def _only(node, allowed):
    if any(value is not None and value is not False and value != [] and key not in allowed
           for key, value in node.args.items()):
        raise ValueError(f'unsupported SQL syntax in {node.key}')


def _column(node):
    if not isinstance(node, exp.Column):
        raise ValueError('expected a public column name; expressions and functions are unsupported')
    _only(node, {'this'})
    if not isinstance(node.this, exp.Identifier) or not node.name:
        raise ValueError('expected a public column name')
    return node.name


def _value(node, field_type):
    sign = 1
    if isinstance(node, exp.Neg):
        sign, node = -1, node.this
    if isinstance(node, exp.Literal):
        _only(node, {'this', 'is_string'})
        if node.is_string:
            if sign < 0: raise ValueError('a string cannot be negated')
            return _literal(node.this)
        text = ('-' if sign < 0 else '') + node.this
        try:
            number = Decimal(text)
            if not number.is_finite(): raise ValueError('numeric literal must be finite')
            if field_type is not None and pa.types.is_decimal(field_type):
                return _literal(number)
            if field_type is not None and pa.types.is_floating(field_type):
                value = float(text)
                if not math.isfinite(value): raise ValueError('floating literal must be finite')
                return _literal(value)
            if number == number.to_integral_value():
                # Avoid materializing arbitrarily large integer exponents.
                if abs(number) >= Decimal(2) ** 64: raise ValueError('integer literal out of range')
                return _literal(int(number))
            if field_type is None:
                raise ValueError('records schema is required to type a fractional numeric literal')
            raise ValueError('fractional literal requires a floating-point or decimal column')
        except (InvalidOperation, OverflowError):
            raise ValueError('invalid numeric literal') from None
    if sign < 0: raise ValueError('expected a numeric literal after minus')
    if isinstance(node, exp.Boolean): return _literal(node.this)
    if isinstance(node, exp.Null):
        raise ValueError('use IS NULL or IS NOT NULL; null literals are unsupported')
    if type(node) is exp.Cast:
        _only(node, {'this', 'to'})
        if not isinstance(node.this, exp.Literal) or not node.this.is_string:
            raise ValueError('only typed date and timestamp string literals are supported')
        target = node.args['to']
        _only(target, {'this', 'nested'})
        kind = target.this.value
        try:
            if kind == 'DATE': return _literal(date.fromisoformat(node.this.this))
            if kind in ('TIMESTAMP', 'TIMESTAMPTZ'):
                value = datetime.fromisoformat(node.this.this.replace('Z', '+00:00'))
                if kind == 'TIMESTAMP' and value.utcoffset() is not None:
                    raise ValueError('use TIMESTAMPTZ for a timestamp with an offset')
                if kind == 'TIMESTAMPTZ' and value.utcoffset() is None:
                    raise ValueError('timestamp with time zone requires an offset')
                return _literal(value)
        except ValueError:
            raise ValueError('invalid date or timestamp literal') from None
    raise ValueError('expected a literal; subqueries, functions, and column comparisons are unsupported')


def _predicate(node, schema, depth=0):
    if depth > 32: raise ValueError('predicate is too complex')
    if isinstance(node, exp.Paren):
        _only(node, {'this'})
        return _predicate(node.this, schema, depth + 1)
    if isinstance(node, (exp.And, exp.Or)):
        _only(node, {'this', 'expression'})
        return pb.Predicate(op='and' if isinstance(node, exp.And) else 'or', children=[
            _predicate(node.this, schema, depth + 1), _predicate(node.expression, schema, depth + 1)])
    if isinstance(node, exp.Not):
        _only(node, {'this'})
        return pb.Predicate(op='not', children=[_predicate(node.this, schema, depth + 1)])
    operators = {exp.EQ: 'eq', exp.NEQ: 'ne', exp.LT: 'lt', exp.LTE: 'le', exp.GT: 'gt', exp.GTE: 'ge'}
    if type(node) in operators or isinstance(node, (exp.In, exp.Is)):
        _only(node, {'this', 'expressions'} if isinstance(node, exp.In) else {'this', 'expression'})
        name = _column(node.this)
        field_type = None
        if schema is not None:
            index = schema.get_field_index(name)
            if index < 0: raise ValueError(f'unknown or ambiguous column: {name}')
            field_type = schema.field(index).type
        if isinstance(node, exp.Is):
            if not isinstance(node.expression, exp.Null): raise ValueError('only IS NULL and IS NOT NULL are supported')
            return pb.Predicate(op='is_null', column=name)
        if isinstance(node, exp.In):
            if len(node.expressions) > 10000: raise ValueError('too many IN values')
            return pb.Predicate(op='in', column=name, values=[_value(v, field_type) for v in node.expressions])
        return pb.Predicate(op=operators[type(node)], column=name, values=[_value(node.expression, field_type)])
    raise ValueError('unsupported WHERE expression; use comparisons, AND, OR, NOT, IN, or null checks')


@dataclass(frozen=True)
class Query:
    path: str
    columns: list | None
    limit: int | None
    where: object

    def predicate(self, schema=None):
        return Predicate(_predicate(self.where, schema)) if self.where is not None else None


def parse_query(sql, resolve_item=None):
    """Parse one SELECT from a quoted logical path or a shell listing number."""
    if len(sql) > 65536: raise ValueError('SQL query is too long')
    try:
        # Numbers are a shell extension to SQL table syntax. Tokenize first so
        # strings/comments containing "FROM 2" cannot be mistaken for a source.
        tokens = sqlglot.tokenize(sql)
        replacements = []
        for previous, token in zip(tokens, tokens[1:]):
            if previous.token_type == TokenType.FROM and token.token_type == TokenType.NUMBER:
                if not token.text.isdecimal():
                    raise ValueError('FROM item number must be a positive integer')
                if resolve_item is None:
                    raise ValueError('item numbers require the latest shell ls result')
                path = _logical_path(resolve_item(token.text))
                replacements.append((token.start, token.end + 1, '"' + path.replace('"', '""') + '"'))
        for start, end, replacement in reversed(replacements):
            sql = sql[:start] + replacement + sql[end:]
        statements = sqlglot.parse(sql)
    except (SqlglotError, RecursionError):
        raise ValueError('invalid SQL; expected SELECT columns FROM "/item" or FROM item-number [WHERE condition] [LIMIT n]') from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise ValueError('expected exactly one SELECT statement')
    query = statements[0]
    if sum(1 for _ in query.walk()) > 4096: raise ValueError('SQL query is too complex')
    _only(query, {'expressions', 'from_', 'where', 'limit'})
    source = query.args.get('from_')
    if source is None: raise ValueError('FROM requires a quoted absolute Item path or a number from the latest ls result')
    _only(source, {'this'})
    table = source.this
    if not isinstance(table, exp.Table): raise ValueError('FROM must identify one Item; subqueries are unsupported')
    _only(table, {'this'})
    identifier = table.this
    if (not isinstance(identifier, exp.Identifier) or not identifier.args.get('quoted')
            or sql[identifier.meta.get('start', 0):][:1] != '"'):
        raise ValueError('quote the absolute Item path with double quotes')
    path = _logical_path(identifier.this)
    if len(query.expressions) == 1 and isinstance(query.expressions[0], exp.Star):
        _only(query.expressions[0], set())
        columns = None
    else:
        columns = [_column(c) for c in query.expressions]
        if not columns or len(set(columns)) != len(columns):
            raise ValueError('SELECT requires unique column names or *')
    limit = None
    if query.args.get('limit') is not None:
        clause = query.args['limit']
        _only(clause, {'expression'})
        value = clause.expression
        if not isinstance(value, exp.Literal) or value.is_string or not value.this.isdecimal():
            raise ValueError('LIMIT must be a nonnegative integer')
        limit = int(value.this)
        if limit > 2**63-1: raise ValueError('LIMIT exceeds int64 range')
    where = query.args.get('where')
    if where is not None: _only(where, {'this'})
    return Query(path, columns, limit, where.this if where is not None else None)
