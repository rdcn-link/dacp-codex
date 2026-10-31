#!/usr/bin/env python3
"""Render the canonical DACP SVG symbol. Requires resvg-py==0.5.0 and Pillow."""
from pathlib import Path
from io import BytesIO
import resvg_py
from PIL import Image

assets = Path(__file__).resolve().parents[1] / 'assets'
source = assets / 'dacp-mark.svg'
logo = resvg_py.svg_to_bytes(svg_path=str(source), width=512, height=512)
(assets / 'dacp-logo.png').write_bytes(logo)
favicon = resvg_py.svg_to_bytes(svg_path=str(source), width=64, height=64)
(assets / 'dacp-favicon.png').write_bytes(favicon)
Image.open(BytesIO(logo)).save(assets / 'dacp-favicon.ico', sizes=[(16,16),(32,32),(48,48),(64,64)])
