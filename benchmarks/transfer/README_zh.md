# 文件传输性能测试

[English](README.md) · [公网 CSV 筛选报告](../../docs/csv-filter-benchmark_zh.md)

此 Linux 程序仅使用本机 IPv4 回环，对比 DACP/TLS、vsftpd FTPS，并提供未加密 FTP 参考。它启动独立的临时服务，不使用远程主机或已有服务。

## 准备

在项目根目录构建服务器（`(cd dacp-server && sbt stage)`）。`dacp-server/target/universal/stage/bin/` 下须存在 `dacp-server` 和 `dacp-admin`。使用独立 Python 环境安装依赖：

```bash
python3 -m venv /tmp/dacp-transfer-venv
/tmp/dacp-transfer-venv/bin/pip install -r benchmarks/transfer/requirements.txt
```

Ubuntu 下可以仅提取 vsftpd，不安装或启动系统服务：

```bash
mkdir -p "$HOME/.local/share/dacp-transfer-tools"
cd "$HOME/.local/share/dacp-transfer-tools"
apt-get download vsftpd
dpkg-deb -x vsftpd_*.deb extracted
ldd extracted/usr/sbin/vsftpd
```

所有动态库须可解析。本测试需要已有的 `bin`（家目录 `/bin`）、`nobody` 账号、OpenSSL、Java，以及非交互 `sudo -n` 权限，用于启动 vsftpd、设置其配置及 chroot 目录的属主、停止独立进程组。不创建系统账号。vsftpd 配置文件必须属于 root。授予权限前请审阅测试程序。

## 运行

在项目根目录运行，输出目录必须尚不存在：

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/suite.py \
  --output "$PWD/.benchmark-runs/transfer-smoke" \
  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \
  --profile smoke --warmups 0 --repeats 1

/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/suite.py \
  --output "$PWD/.benchmark-runs/transfer-final" \
  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \
  --profile final --warmups 1 --repeats 3

/tmp/dacp-transfer-venv/bin/python -m unittest discover -s benchmarks/transfer -p 'test_*.py'
```

smoke 使用 1/4 MiB 大文件及十个 16 KiB 小文件；final 使用 100 MiB/1 GiB 大文件及 1,000 个 64 KiB 小文件。两者均覆盖七种负载、两种接收方式、三种传输方法。默认正式批次包含 168 次运行，其中 42 次预热。逐文件 fsync 较慢，整个测试需要数分钟。源文件约占 1.2 GiB，还需预留并发接收文件及额外空间。

## 测量约定

- 文件为实际写入的确定性伪随机数据。接收端逐文件核验字节数及 SHA-256。
- 四/八路客户端各下载同一个 100 MiB 源文件，吞吐按合计有效负载计算。
- 复用连接在计时外准备，含首个文件元信息。新连接指每次试验每个工作线程创建客户端，不是每个小文件重新连接。正常逐文件元信息请求仍计时。
- `consume` 计数和校验，不写接收文件。`disk-fsync` 额外计入每个文件的写入、flush、fsync 和 close；不对目录 fsync。接收文件在计时后删除。
- 服务准备、源文件生成和复用客户端准备不计时；工作线程启动、传输、解码、元信息、校验和资源采样均计时。保留热缓存，方法顺序按种子随机交错。
- DACP 使用 reader Token；FTP 方法采用只读匿名登录。两种 TLS 方法验证同一证书及主机名。FTPS 的控制和数据通道都加密；libcurl 开启 TLS 会话缓存，vsftpd 不强制复用。未显式配置压缩。
- 客户端 CPU 为整个进程时间。服务端 CPU 为进程树近似差值，含已等待回收的子进程。RSS 每 50 ms 采样，汇总进程 RSS，不是 PSS 或单请求新增内存。主机上的其他活动可能影响结果。

## 产物

被 Git 忽略的 `.benchmark-runs/` 含临时凭据、TLS 私钥、配置、日志、数据及详细结果，不可整体公开。服务器在 `finally` 中停止；如程序被强制终止，应先识别本批次的进程组再清理，不能按通用进程名批量停止服务。

原始传输测试保留为诊断工具，其全量下载报告不纳入公开说明文档。失败保留在原始记录中，中位数仅使用正式成功样本。分享前审查公开产物。源码摘要标识生成报告时的代码快照；运行清单记录实际测试的服务器及 SDK 产物。仅在确认进程停止后删除自己创建的运行目录。

## CSV 属性过滤对比测试

新增场景比较 DACP 服务端记录过滤与 FTP/FTPS 下载过程中由客户端 PyArrow 流式过滤。使用隔离的回环服务器、公开数据、已验证 TLS、确定性 CSV、五种命中阈值及相同的逐列结果校验。结果不落盘。连接建立不计时；读取、解析、过滤、传输和校验均计时。

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/filter_suite.py \
  --output "$PWD/.benchmark-runs/csv-filter-final" \
  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \
  --profile final --warmups 1 --repeats 3
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish_filter.py \
  .benchmark-runs/csv-filter-final/results.json
```

小规模验证使用 `--profile smoke --warmups 0 --repeats 1`。正式数据分别有 65,536 和 655,360 行（约 9、89 MiB），默认共运行 120 次。安装与清理要求同上。报告工具拒绝失败、不完整或未通过结果校验的批次。源数据与临时凭据保留在忽略的运行目录，运行结果不会自动公开发布。

### 在公网运行 CSV 属性筛选测试

`filter_suite.py --remote-config` 使用已有的远程 DACP/TLS、FTPS 和 FTP 服务，不启动本机服务。保留相同的两份 CSV、五档阈值、固定种子顺序、预热及重复次数和逐列精确校验。FTP/FTPS 继续直接从数据 socket 流式解析，没有先完整下载落盘的阶段。测试进程绕过环境代理以测量直连路径。DACP 显式等待连接可用后才开始计时，逐 Item 的常规元数据请求仍计时。

使用新的远程隔离目录，例如 `/var/snap/docker/common/dacp-benchmarks/csv-filter-20261108-wan`，放入：

- `node/`：完全相同的 `dacp-server/target/universal/stage/` 分发包副本。
- `vsftpd`：提取出的可执行文件，运行所需动态库已具备。
- `data/`：原来的 `observations-65536.csv` 和 `observations-655360.csv`。
- `fixtures.json`：此前完整本机 `filter_suite.py` 结果中的 `fixtures` 对象；对照该批次校验源数据、可执行文件及 JAR 摘要。
- `remote_filter_fixture.py`：本目录中的部署脚本。

不要复制既有凭据或 API 配置。在远程使用 root 权限执行：

```bash
python3 remote_filter_fixture.py start \
  --root /var/snap/docker/common/dacp-benchmarks/csv-filter-20261108-wan \
  --public-ip 39.106.98.67 --client-ip 60.245.194.13
```

按需替换允许访问的客户端 IP。端口为 DACP `28801`、FTPS `28802`、FTP `28803`、FTPS 被动端口 `28810–28819`、FTP 被动端口 `28820–28829`。脚本检查端口未被占用、验证源文件摘要、生成 SAN 包含公网 IP 的专用临时证书，并将防火墙规则限制为回环及指定客户端。云防火墙也须允许相关端口。关闭 DACP 看板及 S3，不启动 Union，不创建系统账号或系统服务。只匿名发布确定性的合成数据，FTP 只读；不修改既有应用和官网。

仅通过受信任的 SSH 将 `tls/cert.pem` 复制回本机，记录其本地绝对路径；私钥留在服务器。准备私有的 `remote-config.json`，包含：

- `host`：`39.106.98.67`。
- `ports`：`{"dacp-tls":28801,"ftps":28802,"ftp-plain":28803}`。
- `ca`：已校验证书 `cert.pem` 的本地绝对路径。
- `fixtures`：完整的原始 `fixtures.json`，包含预期行数和逐列摘要。
- `network`：公网直连路径及是否限速的说明。
- `environment`：真实服务端系统、CPU 数量及型号、内存、Java、JVM 堆大小及其他负载。
- `runtime_inventory`：在远程验证的服务端 JAR 及 FTP 可执行文件 SHA-256。

在客户端执行，并显式选择忽略的报告目录：

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/filter_suite.py \
  --remote-config .benchmark-runs/csv-filter-wan-staging/remote-config.json \
  --output .benchmark-runs/csv-filter-wan-20261108 \
  --profile final --warmups 1 --repeats 3
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish_filter_wan.py \
  .benchmark-runs/csv-filter-wan-20261108/results.json \
  --output .benchmark-runs/csv-filter-wan-20261108/report
```

共 120 次运行，包含 30 次预热。独立的公网报告生成器只接受完整成功批次，逐一验证结果，仅向指定目录写入中英文报告、图表和原始测量，不向官方说明文档或官网发布。报告区分实际命中率和名义阈值、Arrow 逻辑内存量和线速字节、完整筛选任务时间和孤立协议速度，并保留包括高命中率下不利于 DACP 的全部结果。

客户端 CPU/RSS 按原方法采样；不采集远程服务端 CPU/RSS，相应字段为 `null`，不对恰巧同 PID 的本机进程进行采样。必须说明硬件、网络及共享服务器负载差异，不能据此推断普遍加速倍数。

结束后使用相同参数，将 `start` 改为 `stop` 关闭远程专用服务。脚本检查登记进程的启动时间，只终止自己的进程组并删除自己的带标记防火墙规则，保留数据和分发包以便重测。不要关闭证书验证，也不要按通用进程名批量终止进程。

### 发布两份已校验的公网报告

在 DACP 仓库中执行，显式发布 CSV 筛选和 CONET 淤地坝两组结果。该编写工具将报告、SVG 及校验后的 JSON 打包到 `docs/`，运行时不依赖 CONET 目录；不要复制源数据或凭据。

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/publish-wan-reports.py \
  --conet-home /home/cnic/conet \
  --check-dams-results /home/cnic/conet/.state/benchmarks/check-dams-wan-verified-20261108/results.json \
  --csv-filter-results .benchmark-runs/csv-filter-wan-20261108/results.json
```
