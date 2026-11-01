# DACP 视觉标识

[English](README.md)

`dacp-mark.svg` 是不含文字的标准图标。细密背景点阵中的点相连形成 A 和 C，分别表达数据访问和协作。点阵右上角向左一列、向下一行的位置有一颗带柔和光晕的小星星。其 `96 × 96` viewBox、圆角背景（`x=2`、`y=2`、`width=92`、`height=92`、`rx=25`）及 `#10243c` 底色与 XStream 一致。官网页首图标在桌面端显示为 `80 × 80` CSS 像素，窄屏为 `64 × 64`。

节点与 Union 看板通过资源生成器打包此文件。独立 DACP 官网将副本保存于 `src/main/resources/web/assets/`，修改后需同步。官网运行时不依赖本项目目录。PNG/ICO 派生文件用于 Markdown、海报与浏览器图标。历史截图继续保留其拍摄时的界面。

重新生成位图资源：

```bash
python3 -m pip install resvg-py==0.5.0 Pillow
python3 scripts/render-branding.py
```

脚本生成 512 像素 PNG、64 像素 favicon，以及包含 16/32/48/64 像素尺寸的 ICO。更新官网时，应将这些文件与 SVG 一起复制。
