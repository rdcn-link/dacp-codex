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
        page.evaluate('([points,value])=>{positions=points;selectedSteps.clear();selected=null;outletSelected=false;nodeAreas={};outletPosition={x:1200,y:800};setInspector(false);zoom(value);render();document.getElementById("canvas").scrollTo(0,0)}', [points, scale])

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

    # Execution bindings on displaced steps remain intact inside a node region.
    page.evaluate("job.tasks.forEach(t=>t.node='nd-a');positions={a:{x:80,y:140},b:{x:400,y:140},c:{x:400,y:300}};nodeAreas={'nd-a':{x:40,y:80}};outletPosition={x:1200,y:800};render()")
    begin(320,0);page.mouse.up();no_overlaps()
    assert page.evaluate("job.tasks.every(t=>t.node==='nd-a')")

    # Outlet and library drops also accept occupied destinations and displace cards.
    layout(original)
    page.evaluate('outletPosition={x:400,y:400};renderOutlet()')
    h=page.locator('#outlet-handle').bounding_box()
    page.mouse.move(h['x']+30,h['y']+20);page.mouse.down();page.mouse.move(h['x']+30,h['y']-240,steps=10);page.mouse.up()
    assert page.evaluate('laneBounds.every(r=>!overlaps(r,outletRect()))')
    assert page.evaluate('positions.b.y')>140
    no_overlaps()
    page.locator('[data-operator="read_item@1.0"]').drag_to(page.locator('[data-task="b"]'))
    expect(page.locator('.card')).to_have_count(4)
    no_overlaps()

    # Whole regions push other regions as rigid groups, plus standalone steps and Outlet.
    region_job={'name':'Region placement','tasks':[dict(id=node+str(i),operator='read_item@1.0',inputs={},params={'uri':'$INPUT_URI'},node='nd-'+node) for node in ['a','b','c'] for i in [1,2]]+[dict(id='free',operator='read_item@1.0',inputs={},params={'uri':'$INPUT_URI'})],'outputs':{}}
    page.evaluate('job=>loadDocument(JSON.stringify(job))',region_job)
    page.evaluate("positions={a1:{x:80,y:140},a2:{x:350,y:140},b1:{x:780,y:140},b2:{x:1050,y:140},c1:{x:780,y:440},c2:{x:1050,y:440},free:{x:1040,y:760}};nodeAreas={'nd-a':{x:40,y:80},'nd-b':{x:740,y:80},'nd-c':{x:740,y:380}};outletPosition={x:1040,y:900};setInspector(false);zoom(.5);render()")
    page.evaluate("nodes=nodes.slice(0,3).map((n,i)=>({...n,id:['nd-a','nd-b','nd-c'][i]}));render()")
    hovered=[]
    for node in ['a','b','c']:
        handle=page.locator('[data-node-region="nd-'+node+'"] .lane-handle')
        handle.hover()
        hovered.append(handle.evaluate("e=>({border:getComputedStyle(e).borderTopColor,background:getComputedStyle(e).backgroundColor,color:getComputedStyle(e).color})"))
    for property in ['border','background','color']:
        assert len({style[property] for style in hovered})==3, (property,hovered)
    before=page.evaluate('snapshot()');history=page.evaluate('({undo:undo.length,redo:redo.length})')
    h=page.locator('[data-node-region="nd-a"] .lane-handle').bounding_box()
    x,y=h['x']+h['width']/2,h['y']+h['height']/2
    page.mouse.move(x,y);page.mouse.down();page.mouse.move(x+350,y,steps=10)
    expect(page.locator('[data-node-region="nd-a"]')).to_have_class('lane selected')
    assert page.locator('[data-node-region="nd-a"]').evaluate("e=>getComputedStyle(e).outlineStyle")=="none"
    assert page.locator('[data-node-region="nd-a"]').evaluate("e=>getComputedStyle(e).boxShadow")!="none"
    assert page.evaluate('positions.b1.y')>140
    assert page.evaluate('positions.c1.y')>440
    assert page.evaluate('positions.free.y')>760
    assert page.evaluate('outletPosition.y')>900
    assert page.evaluate('positions.b2.x-positions.b1.x')==270
    assert page.evaluate('positions.c2.x-positions.c1.x')==270
    page.mouse.up();assert page.evaluate('positions.a1.x')==780
    assert page.evaluate('undo.length')==history['undo']+1
    after_region=page.evaluate('snapshot()')
    no_overlaps()
    assert page.evaluate("job.tasks.filter(t=>t.id!=='free').every(t=>t.node==='nd-'+t.id[0])")
    page.keyboard.press('Control+z');assert page.evaluate('snapshot()')==before
    page.keyboard.press('Control+y');assert page.evaluate('snapshot()')==after_region
    page.keyboard.press('Control+z')
    # A preview restores all displaced groups when moved away or cancelled.
    history=page.evaluate('({undo:undo.length,redo:redo.length})')
    h=page.locator('[data-node-region="nd-a"] .lane-handle').bounding_box()
    x,y=h['x']+h['width']/2,h['y']+h['height']/2
    page.mouse.move(x,y);page.mouse.down();page.mouse.move(x+350,y,steps=10)
    assert page.evaluate('positions.b1.y')>140
    page.mouse.move(x,y+200,steps=10)
    assert page.evaluate('positions.b1.y')==140
    assert page.evaluate('positions.c1.y')==440
    page.dispatch_event('[data-node-region="nd-a"] .lane-handle','pointercancel');page.mouse.up()
    assert page.evaluate('snapshot()')==before
    assert page.evaluate('({undo:undo.length,redo:redo.length})')==history
    # The same region can be placed at a clear location.
    h=page.locator('[data-node-region="nd-a"] .lane-handle').bounding_box()
    page.mouse.move(h['x']+h['width']/2,h['y']+h['height']/2);page.mouse.down();page.mouse.move(h['x']+h['width']/2,h['y']+h['height']/2+200,steps=10);page.mouse.up()
    assert page.evaluate('positions.a1.y')==540
    assert page.evaluate('positions.b1.y')==140
    page.keyboard.press('Control+z');assert page.evaluate('snapshot()')==before
    # Pulling the lower region upward pushes the upper regions upward, preserving order.
    page.evaluate("positions={a1:{x:80,y:560},a2:{x:350,y:560},b1:{x:80,y:860},b2:{x:350,y:860},c1:{x:80,y:260},c2:{x:350,y:260},free:{x:1100,y:100}};nodeAreas={'nd-a':{x:40,y:500},'nd-b':{x:40,y:800},'nd-c':{x:40,y:200}};outletPosition={x:1200,y:1100};zoom(.5);render();document.getElementById('canvas').scrollTo(0,0)")
    before_up=page.evaluate('snapshot()')
    h=page.locator('[data-node-region="nd-b"] .lane-handle').bounding_box()
    x,y=h['x']+h['width']/2,h['y']+h['height']/2
    page.mouse.move(x,y);page.mouse.down();page.mouse.move(x,y-75,steps=10)
    assert page.evaluate("nodeAreas['nd-b'].y")==650
    assert page.evaluate("nodeAreas['nd-a'].y")<500
    assert page.evaluate("nodeAreas['nd-c'].y")<200
    assert page.evaluate("nodeAreas['nd-c'].y<nodeAreas['nd-a'].y && nodeAreas['nd-a'].y<nodeAreas['nd-b'].y")
    assert page.evaluate('positions.a2.x-positions.a1.x')==270
    page.mouse.up();no_overlaps()
    after_up=page.evaluate('snapshot()')
    page.keyboard.press('Control+z');assert page.evaluate('snapshot()')==before_up
    page.keyboard.press('Control+y');assert page.evaluate('snapshot()')==after_up
    # Reaching the top keeps both regions visible without flipping their order.
    assert page.evaluate("""()=>{const rects=[{x:40,y:100,width:650,height:250,fixed:true},{x:40,y:80,width:650,height:250,fixed:false}];avoidRectangles(rects,-1);return rects.every(r=>r.y>=30)&&!overlaps(...rects)&&rects[1].y<rects[0].y;}""")
    # Outlet is always outside complete node regions, including empty container space.
    page.evaluate("positions={a1:{x:80,y:140},a2:{x:350,y:140},b1:{x:780,y:140},b2:{x:1050,y:140},c1:{x:1480,y:140},c2:{x:1750,y:140},free:{x:80,y:600}};nodeAreas={'nd-a':{x:40,y:80},'nd-b':{x:740,y:80},'nd-c':{x:1440,y:80}};outletPosition={x:450,y:250};zoom(.5);render();document.getElementById('canvas').scrollTo(0,0)")
    assert page.evaluate('laneBounds.every(r=>!overlaps(r,outletRect()))')
    # Dragging Outlet into an empty part of a region snaps it outside, without rejecting the move.
    before_outside=page.evaluate('snapshot()')
    page.evaluate("document.getElementById('canvas').scrollLeft=700")
    h=page.locator('#outlet-handle').bounding_box()
    x,y=h['x']+30,h['y']+15
    page.mouse.move(x,y);page.mouse.down();page.mouse.move(x-100,y,steps=10)
    assert page.evaluate('nodeRegionBounds().every(r=>!overlaps(r,outletRect()))')
    page.mouse.up()
    assert page.evaluate('laneBounds.every(r=>!overlaps(r,outletRect()))')
    page.keyboard.press('Control+z');assert page.evaluate('snapshot()')==before_outside
    # Growing a region around Outlet also relocates it; this covers load/arrange/render paths.
    page.evaluate("outletPosition={x:2250,y:250};positions.c2.x=2400;render()")
    assert page.evaluate('laneBounds.every(r=>!overlaps(r,outletRect()))')
    page.evaluate('arrange()')
    assert page.evaluate('laneBounds.every(r=>!overlaps(r,outletRect()))')
    assert not errors,errors
    browser.close()
print('Passed step icons, automatic cascading avoidance, preview restoration, cancellation, undo/redo, zoom, multi-selection, Outlet displacement, node assignments and whole-region placement')
