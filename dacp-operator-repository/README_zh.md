# DACP 算子仓库

[English](README.md)

Scala HTTP/HTTPS 服务，提供公开算子检索下载、仅源码提交、隔离构建及管理员审核。公开界面不提供开发者上传表单。管理员在 `/admin` 登录，管理提交 Token、运行环境与审核。

## 构建与启动

需要 JDK 17、sbt 1.10.x；源码构建另需 Linux 和可访问的 Docker Engine。

```bash
cd dacp-operator-repository
sbt test stage
./target/universal/stage/bin/dacp-operator-repository serve \
  --root /srv/dacp/operators --host 127.0.0.1 --port 8820
```

首次启动将随机管理员密码写入 `<root>/.admin/bootstrap-password`，私有目录权限为 `0700`。日志只显示路径，不显示密码。也可使用 `--admin-password-env ENV_NAME` 指定包含 12–1024 字符的初始密码环境变量；初始化设置不会覆盖已有密码。密码采用带盐 PBKDF2-HMAC-SHA256 校验，提交 Token 仅保存摘要。管理员会话有效八小时，重启清除；写操作要求 CSRF 校验，连续登录失败受到速率限制。

非本机管理员/开发者访问使用 HTTPS。原生 TLS 参数为 `--tls-keystore PKCS12 --tls-password-env ENV_NAME`，HTTPS 会话使用 Secure Cookie。`--publish-max-bytes` 设置环境上传限制，默认 8589934592 字节。源码另限 64 MiB，清单限 64 KiB。

## 开发与发布

[源码提交、构建与审核](../docs/operator-submission_zh.md)包含开发者/管理员完整流程、YAML、命令、接口、隔离和扫描限制；[算子编写](../docs/operator-authoring_zh.md)提供流处理示例。

1. 管理员注册兼容执行/构建环境，按提交者分配 Token。
2. 开发者选择公开环境，提交源码和接口元数据，显式声明是否公开源码。
3. 仓库异步扫描与构建；构建失败保持私有。
4. 管理员检查源码、发现与产物，选择/新增多个分类，确认 0–5 分可信度，默认采用扫描得分。
5. 仅批准后公开算子；只有声明 `source_public: true` 的提交才同时公开源码。

公开入口：`/`、`/operators/{name}/{version}`、`/runtimes`、`/v1/operators`、`/v1/runtimes`、`/v1/categories`。首页合并版本并显示最新已批准版本。搜索包含管理员分类，页面展示管理员最终可信度，详细扫描证据保持私有。分数是审核评价，不是安全保证。下载保留文件名，不能绕过审批或私有源码限制。

源码构建通过模板插件支持 Python、Java、Scala。Python 生成字节码 ZIP，Java/Scala 生成 JAR，依赖保留在共享运行镜像。仓库不接受提交二进制、任意构建命令、开发者分类或直接发布请求。本地打包文件不会自动公开；已有本地产物应重新提交源码并审核，不保留兼容发布路径。

## 存储与恢复

`<root>/.admin/state.json` 保存密码校验数据、Token 摘要、环境注册、不可变提交清单、扫描/构建结果与审核结论。备份整个 root，包括 `.admin` 和 `blobs/sha256`。同一仓库根目录只能由一个进程持有。会话仅保存在内存中。重启后重新处理排队/运行中的构建，已完成审批与最终分数保留。未引用的上传 blob 保持私有并保留，暂未实现自动垃圾回收。

文件采用 SHA-256 寻址，描述中保留原文件名。执行环境和独立构建环境均固定镜像 ID，批准版本不可覆盖。开发者 Token 对应提交者名称；撤销对后续服务调用生效，访问控制由接口执行，与使用何种客户端工具无关。

## 测试及扩展点

```bash
sbt test
```

测试覆盖私有提交、最终可信度、多分类、源码可见性、审批门禁、文件访问、环境目录、Token 隔离/撤销、CSRF、不可变版本、源码/许可证元数据和 HTML 转义。部署验证还会使用真实 Docker 环境检查编译隔离。参见[源码分析与构建插件](../plugins/dacp-plugin-source-analysis/README_zh.md)的 ServiceLoader 注册方法及契约测试。

Python 字节码和 JVM JAR 可能被反编译。`source_public` 控制原始源码材料的公开，不保证可下载代码的算法保密。Python 构建与执行版本必须匹配。
