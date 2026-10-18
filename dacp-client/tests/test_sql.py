from decimal import Decimal
import pyarrow as pa
import pytest

from dacp.sql import parse_query

SCHEMA = pa.schema([('id', pa.int64()), ('amount', pa.float64()), ('price', pa.decimal128(38, 20)),
                    ('name', pa.string()), ('active', pa.bool_()), ('day', pa.date32()),
                    ('stamp', pa.timestamp('us')), ('instant', pa.timestamp('us', tz='UTC'))])


def test_projection_filter_limit_and_typed_numbers():
    q = parse_query('SELECT amount, name FROM "/reports/orders" WHERE id > 1 AND amount <= 12.5 LIMIT 10;')
    assert q.path == '/reports/orders' and q.columns == ['amount', 'name'] and q.limit == 10
    tree = q.predicate(SCHEMA)._message
    assert tree.op == 'and' and tree.children[0].op == 'gt'
    assert tree.children[0].values[0].type == 'integer'
    assert tree.children[1].values[0].type == 'float'
    q = parse_query('select * from "/x" where price = 1.12345678901234567890')
    assert q.columns is None and q.limit is None
    literal = q.predicate(SCHEMA)._message.values[0]
    assert literal.type == 'decimal' and Decimal(literal.value) == Decimal('1.12345678901234567890')


def test_boolean_logic_nulls_membership_strings_and_quoted_identifiers():
    q = parse_query("SELECT name FROM \"/file name\" WHERE NOT (id IN (-1, 2) OR name = 'O''Brien') AND active IS NOT NULL")
    p = q.predicate(SCHEMA)._message
    assert p.op == 'and' and p.children[0].op == 'not'
    or_expr = p.children[0].children[0]
    assert or_expr.op == 'or' and or_expr.children[0].op == 'in'
    assert or_expr.children[0].values[0].value == '-1'
    assert or_expr.children[1].values[0].value == "O'Brien"
    assert p.children[1].children[0].op == 'is_null'
    assert parse_query('SELECT "name" FROM "/x" WHERE active = TRUE').predicate(SCHEMA)._message.values[0].value == 'true'
    assert parse_query('SELECT * FROM "/x" WHERE id NOT IN (1, 2)').predicate(SCHEMA)._message.op == 'not'
    assert parse_query('SELECT * FROM "/x" LIMIT 0').limit == 0


@pytest.mark.parametrize('text,kind', [
    ("day = DATE '2026-01-02'", 'date'),
    ("stamp = TIMESTAMP '2026-01-02 12:34:56.123456'", 'datetime'),
    ("instant = TIMESTAMP WITH TIME ZONE '2026-01-02 12:34:56+02:00'", 'timestamp'),
])
def test_temporal_literals(text, kind):
    p = parse_query('SELECT * FROM "/x" WHERE ' + text).predicate(SCHEMA)._message
    assert p.values[0].type == kind
    if kind == 'timestamp': assert p.values[0].value == '2026-01-02T10:34:56Z'


@pytest.mark.parametrize('sql', [
    'SELECT amount WHERE id > 1', 'SELECT * FROM relative', "SELECT * FROM '/x'",
    'SELECT * FROM "/x"; SELECT * FROM "/y"', 'DELETE FROM "/x"',
    'SELECT DISTINCT name FROM "/x"', 'SELECT id AS n FROM "/x"',
    'SELECT COUNT(*) FROM "/x"', 'SELECT * FROM "/x" ORDER BY id',
    'SELECT id FROM "/x" GROUP BY id', 'SELECT * FROM "/x" LIMIT 1 OFFSET 2',
    'SELECT * FROM "/x" JOIN "/y" ON id = id', 'SELECT * FROM (SELECT * FROM "/x")',
    'WITH x AS (SELECT * FROM "/x") SELECT * FROM x',
    'SELECT * FROM "/x" AS x', 'SELECT x.id FROM "/x"',
    'SELECT * EXCEPT (name) FROM "/x"', 'SELECT id, id FROM "/x"',
    'SELECT * FROM "/../x"', 'SELECT * FROM "/x" LIMIT -1',
    'SELECT * FROM "/x" LIMIT 1.5', 'SELECT * FROM "/x" LIMIT 9223372036854775808',
    'SELECT * FROM "/x" WHERE id = NULL', 'SELECT * FROM "/x" WHERE id = amount',
    'SELECT * FROM "/x" WHERE id + 1 > 2', 'SELECT * FROM "/x" WHERE name LIKE \'a%\'',
    'SELECT * FROM "/x" WHERE id IN (SELECT id FROM "/y")',
    'SELECT * FROM "/x" WHERE id IN (1, NULL)', 'SELECT * FROM "/x" WHERE active IS TRUE',
    'SELECT * FROM "/x" WHERE missing > 1', 'SELECT * FROM "/x" WHERE id > 1.5',
    'SELECT * FROM "/x" WHERE stamp = DATE \'bad\'',
])
def test_unsupported_or_invalid_queries_fail(sql):
    with pytest.raises(ValueError):
        parse_query(sql).predicate(SCHEMA)


def test_numeric_source_resolves_only_from_tokens_and_quotes_paths():
    seen = []
    def resolve(number):
        seen.append(number)
        return '/folder/odd"name'
    query = parse_query("SELECT name FROM /* source */ 2 WHERE name = 'FROM 99' LIMIT 10", resolve_item=resolve)
    assert seen == ['2'] and query.path == '/folder/odd"name'
    assert query.predicate(SCHEMA)._message.values[0].value == 'FROM 99'
    assert parse_query('SELECT * FROM "/x" -- FROM 8', resolve_item=resolve).path == '/x'
    assert seen == ['2']


@pytest.mark.parametrize('number', ['1.5', '1e2', '-1'])
def test_invalid_numeric_sources(number):
    with pytest.raises(ValueError):
        parse_query('SELECT * FROM ' + number, resolve_item=lambda _: '/x')
