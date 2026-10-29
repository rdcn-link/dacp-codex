#!/usr/bin/env python3
"""Browser automatic avoidance and placement checks. No jobs are executed."""
import argparse
from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://localhost:8840')
parser.add_argument('--chromium', required=True)
args = parser.parse_args()
with sync_playwright() as p:
    browser = p.chromium.launch(executable_path=args.chromium, args=['--no-sandbox','--disable-dev-shm-usage'])
    page = browser.new_page(viewport={'width': 1650, 'height': 1100})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.goto(args.url)
    expect(page.locator('#connection')).to_contain_text('Connected', timeout=25000)
    expect(page.locator('#repository-state')).to_contain_text('published', timeout=25000)
    job = {'name': 'Placement checks', 'tasks': [dict(id=id, operator='read_item@1.0', inputs={}, params={'uri': '$INPUT_URI'}) for id in ['a', 'b', 'c']], 'outputs': {}}
    page.evaluate('job=>loadDocument(JSON.stringify(job))', job)

    def layout(points, scale=1):
        page.evaluate('([points,value])=>{positions=points;selectedSteps.clear();selected=null;outletSelected=false;outletPosition={x:1200,y:800};setInspector(false);zoom(value);render();document.getElementById("canvas").scrollTo(0,0)}', [points, scale])

    def begin(dx, dy):
        head = page.locator('[data-task="a"] .card-head').bounding_box()
        x,y = head['x']+head['width']/2,head['y']+head['height']/2
        page.mouse.move(x,y);page.mouse.down();page.mouse.move(x+dx,y+dy,steps=10)
        return x,y

    original = dict(a=dict(x=80,y=140), b=dict(x=400,y=140), c=dict(x=400,y=300))
    layout(original)
    expect(page.locator('.card-head .step-icon')).to_have_count(3)
    card_box=page.locator('[data-task="a"]').bounding_box()
    icon_box=page.locator('[data-task="a"] .step-icon').bounding_box()
    assert icon_box['x']>card_box['x']+card_box['width']-42
    assert icon_box['y']<card_box['y']+22
    source_path=page.locator('[data-operator="read_item@1.0"] svg path').get_attribute('d')
    assert page.locator('[data-task="a"] .step-icon path').get_attribute('d')==source_path

    def no_overlaps():
        assert page.evaluate("""()=>{const rects=dragLayout();return rects.every((r,i)=>rects.slice(i+1).every(s=>!overlaps(r,s)))&&rects.every(r=>!overlaps(r,outletRect()));}""")

    history = page.evaluate('({undo:undo.length,redo:redo.length})')
    begin(320,0)
    expect(page.locator('.drop-invalid')).to_have_count(0)
    assert page.evaluate('positions.a.x')==400
    assert page.evaluate('positions.b.y')>140
    assert page.evaluate('positions.c.y')>300
    no_overlaps()
    page.mouse.up()
    after=page.evaluate('snapshot()')
    assert page.evaluate('undo.length')==history['undo']+1
    expect(page.locator('#inspector')).to_be_hidden()
    page.keyboard.press('Control+z');assert page.evaluate('positions')==original
    page.keyboard.press('Control+y');assert page.evaluate('snapshot()')==after
    page.keyboard.press('Control+z')

    # Moving away restores displaced neighbors; cancellation preserves redo/history.
    history=page.evaluate('({undo:undo.length,redo:redo.length})')
    x,y=begin(320,0)
    page.mouse.move(x,y+240,steps=10)
    assert page.evaluate('positions.b')==original['b']
    assert page.evaluate('positions.c')==original['c']
    page.dispatch_event('[data-task="a"]','pointercancel')
    page.mouse.up()
    assert page.evaluate('positions')==original
    assert page.evaluate('({undo:undo.length,redo:redo.length})')==history
    page.keyboard.press('Control+y');assert page.evaluate('snapshot()')==after
    page.keyboard.press('Control+z')

    # Zoomed drags use canvas coordinates and preserve selected-group offsets.
    layout(original,.7)
    begin(320*.7,0);page.mouse.up()
    assert abs(page.evaluate('positions.a.x')-400)<1
    no_overlaps()
    layout(dict(a=dict(x=80,y=140),b=dict(x=400,y=140),c=dict(x=80,y=270)))
    page.locator('[data-task="a"] .card-head').click()
    page.locator('[data-task="c"] .card-head').click(modifiers=['Shift'])
    begin(320,0);page.mouse.up()
    assert page.evaluate('positions.c.y-positions.a.y')==130
    assert page.evaluate('positions.a.x==400 && positions.c.x==400')
    expect(page.locator('.card.selected')).to_have_count(2)
    no_overlaps()

    # Step dragging also displaces Outlet and restores it with undo.
    layout(dict(a=dict(x=80,y=140),b=dict(x=700,y=140),c=dict(x=700,y=400)))
    page.evaluate('outletPosition={x:400,y:140};renderOutlet()')
    begin(320,0);page.mouse.up()
    assert page.evaluate('positions.a.x')==400
    assert page.evaluate('outletPosition.y')>140
    no_overlaps()
    page.keyboard.press('Control+z');assert page.evaluate('outletPosition')==dict(x=400,y=140)

    # Outlet and library drops also accept occupied destinations and displace cards.
    layout(original)
    page.evaluate('outletPosition={x:400,y:400};renderOutlet()')
    h=page.locator('#outlet-handle').bounding_box()
    page.mouse.move(h['x']+30,h['y']+20);page.mouse.down();page.mouse.move(h['x']+30,h['y']-240,steps=10);page.mouse.up()
    assert page.evaluate('positions.b.y')>140
    no_overlaps()
    page.locator('[data-operator="read_item@1.0"]').drag_to(page.locator('[data-task="b"]'))
    expect(page.locator('.card')).to_have_count(4)
    no_overlaps()

    assert not errors,errors
    browser.close()
print('Passed step icons, automatic cascading avoidance, restoration, cancellation, undo/redo, zoom, multi-selection and Outlet displacement')
