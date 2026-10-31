# DACP visual identity

[中文](README_zh.md)

`dacp-mark.svg` is the canonical text-free symbol. Separate white A and C letters represent access and collaboration; four colored nodes linked around them represent cooperating data nodes. Its `96 × 96` viewBox, rounded background (`x=2`, `y=2`, `width=92`, `height=92`, `rx=25`) and `#10243c` background match XStream. Website header symbols render at `37 × 37` CSS pixels.

Node and Union dashboards package this asset through their resource generators. The independent DACP website vendors a copy in `src/main/resources/web/assets/`; synchronize it on changes. The website has no runtime dependency on this checkout. PNG/ICO derivatives support Markdown, posters and browser favicons. Historical screenshots remain captures of their original interface.

Regenerate raster assets with:

```bash
python3 -m pip install resvg-py==0.5.0 Pillow
python3 scripts/render-branding.py
```

This writes a 512-pixel PNG, a 64-pixel favicon and an ICO with 16/32/48/64-pixel sizes. Copy these with the SVG when updating the website.
