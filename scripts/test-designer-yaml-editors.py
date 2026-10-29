#!/usr/bin/env python3
"""Browser checks for YAML editing, draft synchronization, export and sidebar help."""
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:8840')
parser.add_argument('--chromium', required=True)
args = parser.parse_args()
workflow = '''name: Editor test
tasks:
  - id: read
    operator: read_item@1.0
    inputs: {}
    params: {uri: $INPUT_URI, view: bytes}
outputs: {result: read}
'''
variables = '''# Input values
INPUT_URI: dacp://localhost:19860/weather/observations

COUNT: 123
ENABLED: true
LABEL: '001'
'''

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(executable_path=args.chromium,
        args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'])
    page = browser.new_page(viewport={'width': 1450, 'height': 950})
    errors = []
    external = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.on('request', lambda request: external.append(request.url)
        if not request.url.startswith((args.url, 'blob:')) else None)
    page.route('**/api/operators', lambda route: route.fulfill(json=[]))
    page.route('**/api/infer', lambda route: route.fulfill(json={
        'tasks': {'read': {'outputs': {'result': {
            'certainty': 'unknown', 'representations': ['bytes'], 'formats': []}}, 'diagnostics': []}},
        'outputs': {}}))
    page.goto(args.url)
    page.wait_for_function('()=>config?.generator', polling=100)
    page.evaluate('(text)=>loadDocument(text)', workflow)
    page.locator('[data-tab="yaml"]').dispatch_event('click')
    expect(page.locator('#yaml .cm-content')).to_be_visible()
    expect(page.locator('#yaml .yaml-key').first).to_be_visible()
    expect(page.locator('#yaml .yaml-variable')).to_contain_text('$INPUT_URI')
    assert page.locator('#yaml .cm-lineNumbers').count() == 1
    assert page.locator('#yaml .cm-foldGutter').count() == 1
    assert not page.evaluate('yamlEdited'), 'Programmatic synchronization is not an edit'

    # Editing uses a separate history, and later graph synchronization cannot erase a draft.
    original = page.evaluate('jobYamlEditor.getText()')
    graph_history = page.evaluate('undo.length')
    content = page.locator('#yaml .cm-content')
    content.fill(workflow.replace('Editor test', 'Edited YAML'))
    assert page.evaluate('yamlEdited')
    content.press('Control+z')
    assert page.evaluate('jobYamlEditor.getText()') == original
    assert page.evaluate('undo.length') == graph_history
    content.fill(workflow.replace('Editor test', 'Draft kept'))
    draft = page.evaluate('jobYamlEditor.getText()')
    page.evaluate('async()=>{job.name="Changed graph";await syncYaml()}')
    assert page.evaluate('jobYamlEditor.getText()') == draft
    page.locator('#apply-yaml').dispatch_event('click')
    page.wait_for_function('()=>!yamlEdited && job.name==="Draft kept"', polling=100)
    assert len(page.evaluate('job.tasks')) == 1

    content.press('Control+f')
    expect(page.locator('#yaml .cm-search')).to_be_visible()
    page.locator('#yaml .cm-search input').first.press('Escape')
    expect(page.locator('#yaml .cm-search')).to_have_count(0)
    content.fill('name: [unclosed\n')
    expect(page.locator('#yaml .cm-lintRange-error, #yaml .cm-lintPoint-error').first).to_be_attached(timeout=10000)
    page.evaluate('(text)=>loadDocument(text)', workflow)

    # Variables remain typed YAML; comments and empty lines survive editor round trips.
    page.locator('[data-tab="variables"]').dispatch_event('click')
    var_content = page.locator('#variables .cm-content')
    var_content.fill(variables)
    assert page.evaluate('variablesEditor.getText()') == variables
    expect(page.locator('#variables .yaml-comment')).to_contain_text('# Input values')
    expect(page.locator('#variables .yaml-number')).to_contain_text('123')
    expect(page.locator('#variables .yaml-literal')).to_contain_text('true')
    parsed = page.evaluate('async()=>api("mapping",{yaml:variablesEditor.getText()})')
    assert parsed['COUNT'] == 123 and parsed['ENABLED'] is True and parsed['LABEL'] == '001'
    assert page.evaluate('JSON.parse(localStorage.getItem("xstream-designer")).variables') == variables

    # Help is rendered in a shared lower region, never in browser title tooltips.
    page.evaluate('()=>{selected="read";inspect();setInspector(true);inspectorTab("parameters")}')
    uri_help = page.get_by_role('button', name='Show help for params.uri', exact=True)
    assert uri_help.get_attribute('title') is None
    uri_help.dispatch_event('mouseenter')
    expect(page.locator('#parameter-description')).to_contain_text('Public dataset Item URI')
    page.get_by_role('button', name='Show help for params.view', exact=True).focus()
    expect(page.locator('#parameter-description')).to_contain_text('Options:')
    assert page.locator('#parameter-description').count() == 1

    # Preview is searchable but immutable; all export operations use the original text.
    page.locator('#export').dispatch_event('click')
    expect(page.locator('#export-dialog')).to_be_visible()
    expect(page.locator('#export-preview .cm-content')).to_have_attribute('contenteditable', 'false')
    expect(page.locator('#export-preview .yaml-key').first).to_be_visible()
    full = page.evaluate('exportYamlEditor.getText()')
    assert 'designer:' in full and 'exported_at:' in full
    preview = page.locator('#export-preview .cm-content')
    preview.focus()
    preview.press('Delete')
    assert page.evaluate('exportYamlEditor.getText()') == full
    page.locator('#export-designer').dispatch_event('click')
    plain = page.evaluate('exportYamlEditor.getText()')
    assert 'designer:' not in plain
    page.locator('#export-copy').dispatch_event('click')
    expect(page.locator('#export-status')).to_have_text('Copied.')
    with page.expect_download(timeout=10000) as download:
        page.locator('#export-download').click(force=True)
    assert Path(download.value.path()).read_text() == plain
    page.evaluate('''()=>{window.printProbe={};window.open=()=>({document:{set title(v){},
        createElement:()=>({style:{},set textContent(v){printProbe.text=v}}),
        body:{append(){}},close(){}},focus(){},print(){printProbe.called=true}})}''')
    page.locator('#export-print').dispatch_event('click')
    assert page.evaluate('printProbe.called && printProbe.text===selectedExport()')
    page.locator('#export-designer').dispatch_event('click')
    assert page.evaluate('exportYamlEditor.getText()') == full
    layout = page.evaluate('designLayout()')
    page.locator('#export-close').dispatch_event('click')
    page.evaluate('(text)=>loadDocument(text)', full)
    restored = page.evaluate('designLayout()')
    for key in ['positions', 'outlet', 'viewport']:
        assert restored[key] == layout[key], key
    assert not errors, errors
    assert not external, external
    browser.close()
    print('YAML highlighting, drafts, typed variables, history, search, diagnostics, help and export passed')
