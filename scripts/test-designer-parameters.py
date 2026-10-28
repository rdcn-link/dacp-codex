#!/usr/bin/env python3
"""Browser contract checks for the typed parameter editor (no running services required)."""
import argparse
import json
from pathlib import Path
from playwright.sync_api import sync_playwright

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--chromium')
args = parser.parse_args()
with sync_playwright() as p:
    options = {'args': ['--no-sandbox','--disable-dev-shm-usage']}
    if args.chromium:
        options['executable_path'] = args.chromium
    browser = p.chromium.launch(**options)
    page = browser.new_page()
    page.set_content('<div id="params"></div>')
    page.add_script_tag(path=str(Path(__file__).resolve().parents[1] / 'dacp-job-designer/src/main/resources/web/parameters.js'))
    original = {'text': 'a:\n  YAML stays text', 'count': 123, 'flag': False, 'missing': None,
                'variable': '$INPUT_URI', 'nested': {'values': [1, True, {'x': 'y'}]}, '__proto__': 'safe'}
    page.evaluate('(v) => {window.editor = ParameterEditor.mount(document.querySelector("#params"), JSON.parse(v))}', json.dumps(original))
    assert json.loads(page.evaluate('JSON.stringify(editor.read())')) == original
    # A string resembling a number stays a string; explicit number conversion is typed.
    page.locator('#params').evaluate('(host) => window.editor = ParameterEditor.mount(host, {limit: "123"})')
    assert page.evaluate('editor.read().limit') == '123'
    page.get_by_label('Parameter type', exact=True).select_option('number')
    page.get_by_label('Parameter value', exact=True).fill('42.5')
    assert page.evaluate('editor.read().limit') == 42.5
    page.get_by_label('Parameter value', exact=True).fill('')
    assert page.evaluate('() => {try {editor.read(); return false} catch {return true}}')
    page.get_by_label('Parameter type', exact=True).select_option('variable')
    page.get_by_label('Parameter value', exact=True).fill('$LIMIT')
    assert page.evaluate('editor.read().limit') == '$LIMIT'
    page.get_by_label('Parameter value', exact=True).fill('invalid')
    assert page.evaluate('() => {try {editor.read(); return false} catch {return true}}')
    page.get_by_label('Parameter value', exact=True).fill('$LIMIT')
    page.get_by_role('button', name='Add parameter', exact=True).click(force=True)
    assert page.get_by_label('Parameter name', exact=True).count()==0
    page.get_by_label('New parameter name', exact=True).fill('limit')
    page.get_by_role('button', name='Add', exact=True).click(force=True)
    assert page.locator('.parameter-create .parameter-error').inner_text()
    page.get_by_label('New parameter name', exact=True).fill('extra')
    page.get_by_role('button', name='Add', exact=True).click(force=True)
    assert page.get_by_label('New parameter name', exact=True).count()==0
    assert page.locator('.parameter-name').all_text_contents()==['limit','extra']
    assert page.evaluate('editor.read()') == {'limit': '$LIMIT', 'extra': ''}
    page.get_by_role('button', name='Remove parameter', exact=True).nth(1).click(force=True)
    assert page.evaluate('editor.read()') == {'limit': '$LIMIT'}
    contract = {'type': 'object', 'properties': {
        'limit': {'type': 'integer', 'minimum': 1, 'maximum': 100, 'description': 'Row limit'},
        'mode': {'type': 'string', 'enum': ['fast', 'safe'], 'default': 'safe'},
        'mapping': {'type': 'object', 'properties': {}, 'required': [], 'additionalProperties': {'type': 'number'}},
        'columns': {'type': 'array', 'items': {'type': 'string'}, 'minItems': 1},
        'schema': {'type': ['string', 'object'], 'properties': {}, 'required': [], 'additionalProperties': {'type': 'string'}}
    }, 'required': ['limit'], 'additionalProperties': False}
    page.evaluate('(s) => window.editor = ParameterEditor.mount(document.querySelector("#params"), {limit: 3}, s)', contract)
    assert page.evaluate('editor.read()') == {'limit': 3}, 'Optional defaults must not be injected'
    help_button=page.get_by_role('button',name='Show help for params.limit',exact=True)
    assert help_button.get_attribute('title') is None
    help_button.dispatch_event('mouseenter')
    help_text=page.locator('#parameter-description').inner_text()
    assert 'Row limit' in help_text and 'minimum: 1' in help_text and 'maximum: 100' in help_text
    assert page.locator('.parameter-name').first.get_attribute('title') is None
    page.get_by_role('button',name='Show help for params.mode',exact=True).focus()
    help_text=page.locator('#parameter-description').inner_text()
    assert 'params.mode' in help_text and 'Default: "safe"' in help_text and 'Options:' in help_text
    assert page.locator('#parameter-description').count()==1

    page.get_by_label('Set mode', exact=True).check(force=True)
    assert page.evaluate('editor.read().mode') == 'safe'
    page.get_by_role('group', name='limit', exact=True).get_by_label('Parameter value', exact=True).fill('0')
    assert page.evaluate('() => {try {editor.read(); return false} catch {return true}}')
    page.get_by_role('group', name='limit', exact=True).get_by_role('button', name='Use variable', exact=True).click(force=True)
    page.get_by_role('group', name='limit', exact=True).get_by_label('Parameter value', exact=True).fill('$LIMIT')
    assert page.evaluate('editor.read().limit') == '$LIMIT'
    limit=page.get_by_role('group', name='limit', exact=True)
    assert limit.locator('.parameter-name').inner_text()=='limit'
    assert limit.get_by_label('Parameter type', exact=True).count()==0
    limit.get_by_role('button',name='Use literal value',exact=True).click(force=True)
    assert limit.get_by_label('Parameter value',exact=True).input_value()=='0'
    limit.get_by_label('Parameter value',exact=True).fill('5')
    assert page.evaluate('editor.read().limit')==5
    limit.locator('.parameter-help').dispatch_event('mouseenter')
    assert 'Row limit' in page.locator('#parameter-description').inner_text()
    assert limit.locator('.parameter-help').get_attribute('title') is None
    for value in [{'limit': 1, 'unexpected': 2}, {'limit': 1, 'columns': []}, {'limit': 1, 'mapping': {'x': 'bad'}}]:
        page.evaluate('(x) => window.editor = ParameterEditor.mount(document.querySelector("#params"), x.value, x.schema)', {'value': value, 'schema': contract})
        assert page.evaluate('() => {try {editor.read(); return false} catch {return true}}')
    value = {'limit': 1, 'mapping': {'x': 1.5}, 'columns': ['temperature'], 'schema': {'temperature': 'float64'}}
    page.evaluate('(x) => window.editor = ParameterEditor.mount(document.querySelector("#params"), x.value, x.schema)', {'value': value, 'schema': contract})
    assert page.evaluate('editor.read()') == value
    page.evaluate("ParameterEditor.validate({type: ['string','number'], minimum: 10}, 'x', 'union')")
    assert page.evaluate("() => {try {ParameterEditor.validate({type: ['string','number'], minimum: 10}, 2, 'union'); return false} catch {return true}}")
    browser.close()
print('PASS: typed values, nested values, help region, safe keys, edits, variables, validation, add/remove')
