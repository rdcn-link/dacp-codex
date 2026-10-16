"""Read-only, versioned operator catalog. Image archives are Docker save files."""
import argparse
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import tempfile

PART = re.compile(r"[A-Za-z0-9_-][A-Za-z0-9_.-]*\Z")


def parts(reference):
    values = reference.split("@")
    if len(values) != 2 or not all(PART.fullmatch(v) for v in values):
        raise ValueError("operator must be name@version")
    return values


def publish(root, reference, image):
    name, version = parts(reference)
    parent = Path(root).resolve() / name
    parent.mkdir(parents=True, exist_ok=True)
    target = parent / version
    if target.exists():
        raise FileExistsError("operator version already published")
    with tempfile.TemporaryDirectory(dir=parent) as temporary:
        stage = Path(temporary)
        image_id = subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}", image], text=True).strip()
        # Pin the export to its inspected image ID, never a mutable tag.
        subprocess.run(["docker", "save", "-o", str(stage / "image.tar"), image_id], check=True)
        digest = hashlib.sha256()
        with (stage / "image.tar").open("rb") as source:
            while block := source.read(1024 * 1024):
                digest.update(block)
        digest = digest.hexdigest()
        manifest = dict(api_version=1, name=name, version=version, image=image_id,
                        archive=dict(sha256=digest, size=(stage / "image.tar").stat().st_size))
        (stage / "manifest.json").write_text(json.dumps(manifest))
        # Nonempty directories cannot replace an existing published version.
        stage.rename(target)
    return manifest


def handler(root):
    root = Path(root).resolve()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"ok\n")
                return
            match = re.fullmatch(r"/v1/operators/([^/]+)/([^/]+)(/image.tar)?", self.path)
            if not match:
                self.send_error(404)
                return
            name, version, archive = match.groups()
            try:
                parts(name + "@" + version)
                path = (root / name / version / ("image.tar" if archive else "manifest.json")).resolve()
                if not path.is_relative_to(root):
                    raise ValueError("invalid path")
                with path.open("rb") as source:
                    self.send_response(200)
                    self.send_header("Content-Length", str(path.stat().st_size))
                    self.send_header("Content-Type", "application/octet-stream" if archive else "application/json")
                    self.end_headers()
                    shutil.copyfileobj(source, self.wfile, 1024 * 1024)
            except (ValueError, FileNotFoundError, IsADirectoryError):
                self.send_error(404)

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    pub = sub.add_parser("publish")
    pub.add_argument("reference")
    pub.add_argument("--image", required=True)
    pub.add_argument("--root", required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--root", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8820)
    serve.add_argument("--tls-cert")
    serve.add_argument("--tls-key")
    args = parser.parse_args()
    if args.command == "publish":
        print(json.dumps(publish(args.root, args.reference, args.image)))
    else:
        server = ThreadingHTTPServer((args.host, args.port), handler(args.root))
        if args.tls_cert or args.tls_key:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(args.tls_cert, args.tls_key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
        server.serve_forever()
