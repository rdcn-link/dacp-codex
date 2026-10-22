#!/usr/bin/env python3
"""Check an English DACP website's internal pages and packaged documentation links."""
import argparse
from collections import deque
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urldefrag
from urllib.request import urlopen

class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = []
    def handle_starttag(self, tag, attrs):
        data = dict(attrs)
        if tag == 'a' and 'href' in data:
            self.urls.append(data['href'])

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('origin', nargs='?', default='http://localhost:8830')
    args = parser.parse_args()
    origin = args.origin.rstrip('/')
    pending = deque([origin + '/'])
    seen = set()
    errors = []
    while pending:
        url = pending.popleft()
        if url in seen:
            continue
        seen.add(url)
        try:
            with urlopen(url, timeout=20) as response:
                assert response.status == 200
                if 'text/html' not in response.headers.get('Content-Type', ''):
                    continue
                body = response.read().decode()
            assert '<html lang="en">' in body, url
            page = Links()
            page.feed(body)
            for href in page.urls:
                target = urldefrag(urljoin(url, href))[0]
                if urlsplit(target).netloc == urlsplit(origin).netloc and not urlsplit(target).path.startswith('/download/') and target not in seen:
                    pending.append(target)
        except Exception as exc:
            errors.append(f'{url}: {exc}')
    if errors:
        raise SystemExit('\n'.join(errors))
    print(f'{len(seen)} website and documentation pages checked; no broken internal links')

if __name__ == '__main__':
    main()
