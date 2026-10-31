# DACP 视觉标识

[English](README.md)

`dacp-mark.svg` 是不带文字的标准图形标识。清晰分开的白色 A、C 代表访问与协作，四周相连的四个彩色节点代表参与协作的数据节点。其 `96 × 96` viewBox、圆角底框（`x=2`、`y=2`、`width=92`、`height=92`、`rx=25`）及 `#10243c` 底色均与 XStream 一致。网站页头图形以 `37 × 37` CSS 像素展示。

节点与 Union 看板通过资源生成器打包此文件。独立 DACP 官网将副本保存于 `src/main/resources/web/assets/`，修改后需同步。官网运行时不依赖本项目目录。PNG/ICO 派生文件用于 Markdown、海报与浏览器图标。历史截图继续保留其拍摄时的界面。

重新生成位图资源：

```bash
python3 -m pip install resvg-py==0.5.0 Pillow
python3 scripts/render-branding.py
```

脚本生成 512 像素 PNG、64 像素 favicon，以及包含 16/32/48/64 像素尺寸的 ICO。更新官网时，应将这些文件与 SVG 一起复制。
