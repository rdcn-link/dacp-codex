# 文件传输性能测试

[English](README.md) · [实测报告](../../docs/transfer-benchmark_zh.md)

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

/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish.py \
  .benchmark-runs/transfer-final/results.json
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

`publish.py` 接受完整正式批次，在 `docs/` 下生成中英文报告、公开测量记录和 SVG 图。失败保留在原始记录中，中位数仅使用正式成功样本。分享前审查公开产物。源码摘要标识生成报告时的代码快照；运行清单记录实际测试的服务器及 SDK 产物。仅在确认进程停止后删除自己创建的运行目录。
