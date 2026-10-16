import hashlib
import json
import threading
import urllib.request
import urllib.error
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
import pytest
from dacp_operator_library import handler, parts, publish


def test_reference_validation():
    for value in ('../x@1', 'x@..', 'x@y/z', 'x', 'x@1@2'):
        with pytest.raises(ValueError):
            parts(value)


def test_publish_and_http(tmp_path):
    image = 'sha256:' + 'a' * 64
    def save(command, **kwargs):
        Path(command[3]).write_bytes(b'image archive')
    with patch('subprocess.check_output', return_value=image + '\n'), patch('subprocess.run', side_effect=save):
        manifest = publish(tmp_path, 'identity@1.0', 'example:1')
        with pytest.raises(FileExistsError):
            publish(tmp_path, 'identity@1.0', 'example:1')
    assert manifest['archive']['sha256'] == hashlib.sha256(b'image archive').hexdigest()
    server = ThreadingHTTPServer(('127.0.0.1', 0), handler(tmp_path))
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    base = f'http://127.0.0.1:{server.server_port}'
    try:
        with urllib.request.urlopen(base + '/v1/operators/identity/1.0') as response:
            assert json.load(response) == manifest
        with urllib.request.urlopen(base + '/v1/operators/identity/1.0/image.tar') as response:
            assert response.read() == b'image archive'
        for path in ('/v1/operators/../1.0', '/v1/operators/missing/1', '/etc/passwd'):
            with pytest.raises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(base + path)
            assert error.value.code == 404
    finally:
        server.shutdown()
        thread.join()
        server.server_close()
