# DACP 官方网站

独立的 Scala HTTP 服务，网站内容仅使用英文。提供项目介绍、用户／管理员／开发者文档、服务器和客户端下载。沿用项目 logo 和 C 形 favicon，不依赖外部字体、脚本或 CDN。

## 构建与启动

要求 JDK 17、sbt 1.10.x。必须在模块目录运行 sbt：

```bash
cd dacp-website
sbt test stage
target/universal/stage/bin/dacp-website --host 0.0.0.0 --port 8830 --downloads /srv/dacp/downloads
```

打开 `http://localhost:8830/`。`/docs` 是文档导航，`/downloads` 列出下载目录中实际存在的受支持软件包；`/guide/docs/users/README_en.md` 等路径提供渲染后的文档。默认监听 `0.0.0.0:8830`，默认下载目录是启动工作目录下的 `downloads`。

官网不承担数据访问、任务执行、成员调度或算子发布职责。部署到公网时，可在反向代理上配置域名和 HTTPS；下载目录只允许管理员写入。所有网页和下载均公开访问。

## 准备软件下载

在项目根目录执行以下命令，按顺序构建各组件并生成文件校验清单：

```bash
python3 scripts/prepare-website-downloads.py --output /srv/dacp/downloads
```

脚本需要 sbt、Python 3.10+、pip 和网络。仅构建 `dacp-server`、`dacp-union-server` 的 ZIP 安装包，以及包含 `dacp-shell` 的 `dacp-client` Python wheel，并生成 `SHA256SUMS`。产物为开发快照；客户端依赖由 pip 安装。下载清单仅列出这三个软件组件，大小使用 1024 字节进制：MB 保留一位小数（如 `134.9MB`），kB 显示整数（如 `28kB`），不足 1 kB 显示字节数。官网、源码、Scala SDK 和协作 SDK 包不列入下载清单。

使用 `--skip-build` 可重用服务器 ZIP；客户端 wheel 始终重新生成。缺失服务器安装包会导致失败。文件逐个原子替换，网站无需重启即可发现新文件；整个目录的更新不保证事务一致，请在发布完成后校验。已有无关文件不会被删除，但网站不再列出或提供它们的下载。

## 文档维护

网站构建时收集仓库的 `*_en.md`，排除隐藏路径和构建目录。文档随网站安装包分发，不读取运行机器的源码目录。角色指南、协议、目录、流、算子、插件、部署、访问日志和执行历史等文档均可在线阅读。Markdown 表格受支持，原始 HTML 被转义；本地文档链接保留站内跳转，其他源码／配置链接指向 GitHub。更新文档后重新构建并部署网站。

```bash
cd dacp-website
sbt test Universal/packageBin
```

测试覆盖主要页面、打包文档、下载内容和文件名、路径越界、符号链接、缺失资源及 favicon。
