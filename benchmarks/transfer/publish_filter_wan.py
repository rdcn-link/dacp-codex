#!/usr/bin/env python3
"""Render a verified WAN filtering batch to an explicitly selected directory."""
import argparse
import json
from pathlib import Path
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from publish_filter import validate,LABELS,COLORS
from suite import METHODS

THRESHOLDS=[10,100,1000,5000,10000]
def publish(source,output):
    data=validate(json.loads(source.read_text()))
    if 'remote_environment' not in data:raise ValueError('Remote deployment metadata required')
    output.mkdir(parents=True,exist_ok=True)
    (output/'results.json').write_text(json.dumps(data,indent=2)+'\n')
    files=data['fixtures'];samples=[s for s in data['samples'] if not s['warmup']];cfg=data['config']
    def vals(name,t,m,field):return [s[field] for s in samples if s['file']==name and s['threshold']==t and s['method']==m]
    def med(name,t,m,field='wall_s'):return statistics.median(vals(name,t,m,field))
    for zh in [False,True]:
        suffix='_zh' if zh else ''
        def tr(en,cn):return cn if zh else en
        plt.rcParams.update({'font.family':'WenQuanYi Zen Hei' if zh else 'DejaVu Sans','svg.fonttype':'path'})
        for image,field,label in [('wall','wall_s',tr('Completion time (s)','完成耗时（秒）')),('first-result','first_result_s',tr('First matching batch (s)','首批匹配结果（秒）')),('client-cpu','client_cpu_s',tr('Client CPU time (s)','客户端 CPU 时间（秒）'))]:
            fig,axes=plt.subplots(1,len(files),figsize=(12,4.3),squeeze=False)
            for ax,(name,f) in zip(axes[0],files.items()):
                for i,m in enumerate(METHODS):
                    ys=[med(name,t,m,field) for t in THRESHOLDS]
                    lo=[y-min(vals(name,t,m,field)) for t,y in zip(THRESHOLDS,ys)]
                    hi=[max(vals(name,t,m,field))-y for t,y in zip(THRESHOLDS,ys)]
                    ax.bar([j+(i-1)*.25 for j in range(5)],ys,width=.24,color=COLORS[i],label=LABELS[m],yerr=[lo,hi],capsize=2)
                ax.set_xticks(range(5),['0.1%','1%','10%','50%','100%']);ax.set_xlabel(tr('Requested bucket fraction','请求的 bucket 比例'));ax.set_ylabel(label)
                ax.set_title(f"{f['rows']:,} "+tr('rows','行')+f" / {f['bytes']/1048576:.2f} MiB");ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
            axes[0][0].legend();fig.tight_layout();fig.savefig(output/(image+suffix+'.svg'));plt.close(fig)
        lines=[tr('# CSV attribute filtering over the public WAN','# 公网 CSV 大文件属性筛选对比'),'',('[English](report.md)' if zh else '[中文](report_zh.md)'),'',data['timestamp'],'',
          tr('DACP/TLS, FTPS and FTP run on **39.106.98.67**. The client runs on the original local machine. This compares server-side DACP filtering with streaming client-side FTP/FTPS filtering. Both paths consume and validate all matching rows; neither saves results to disk.','DACP/TLS、FTPS 和 FTP 均部署在 **39.106.98.67**，客户端仍在原本机。对比 DACP 服务端筛选与 FTP/FTPS 客户端流式筛选；双方均消费并校验全部匹配记录，不将结果落盘。'),'',
          '```mermaid','flowchart LR','    CSV[Remote CSV] --> D[DACP server scan + filter]','    D --> A[Arrow matching rows over TLS]','    A --> V[Local full-result validation]','    CSV --> F[FTP / FTPS full CSV transfer]','    F --> P[Local PyArrow parse + filter]','    P --> V','```','',tr('## Environment and verification','## 环境与校验'),'',
          '```json',json.dumps(dict(client=data['host'],server=data['remote_environment']),ensure_ascii=False,indent=2),'```','',
          tr('The client has 16 logical CPUs and the server has 8; hardware differs and both machines share resources with other services. No bandwidth shaping, SSH tunnel or network proxy is used. These results describe this particular public link and workload, not universal protocol speed.','客户端 16 个逻辑 CPU，服务端 8 个；硬件不同，两端均与其他服务共享资源。未限速，不使用 SSH 隧道或网络代理。结果描述本次公网链路和任务，不能作为普遍的协议速度结论。'),'',
          tr('Original fixture SHA-256 values, server JAR and vsftpd binary match the preceding local experiment. Both encrypted methods validate the same pinned self-signed certificate with a public-IP SAN. No certificate validation is disabled. The dataset is public, FTP is read-only, and scoped firewall rules admit only the benchmark client and loopback.','原始数据 SHA-256、服务端 JAR 和 vsftpd 可执行文件均与此前本机测试一致。两种加密方法均校验同一份固定信任的自签证书，证书 SAN 包含公网 IP，没有关闭证书验证。数据集公开、FTP 只读，限定防火墙规则仅允许测试客户端及回环访问。'),'',
          tr('## Data and query','## 数据与查询'),'']
        for name,f in files.items():lines += [f"- `{name}`: {f['rows']:,} "+tr('rows','行')+f"; {f['bytes']:,} bytes ({f['bytes']/1048576:.2f} MiB); SHA-256 `{f['sha256']}`."]
        lines += ['',tr('Schema: `id:int64`, `bucket:int64`, `quality:int64`, `payload:string`; `bucket = id % 10000`, with a deterministic 128-character payload. All four columns are returned. No index, projection or row limit is used.','字段为 `id:int64`、`bucket:int64`、`quality:int64`、`payload:string`；`bucket = id % 10000`，payload 为确定性的 128 字符文本。返回所有四列，无索引、投影或行数限制。'),'',
          '```sql','SELECT * FROM source WHERE bucket <= :threshold','```','',
          tr('Thresholds are 9, 99, 999, 4999 and 9999. DACP uses the typed SDK predicate; FTP/FTPS use the equivalent PyArrow comparison. CSV still requires a complete server scan in the DACP path.','阈值为 9、99、999、4999、9999。DACP 使用 SDK 类型化谓词，FTP/FTPS 使用等价 PyArrow 比较。DACP 路径仍需在服务端完整扫描 CSV。'),'',
          tr(f"Every combination has {cfg['warmups']} warmups and {cfg['repeats']} measured repeats, interleaved using seed {cfg['seed']}. All {len(data['samples'])} trials pass exact row-count and per-column SHA-256 checks, independent of batch boundaries. No floating-point tolerance is used.",f"每种组合预热 {cfg['warmups']} 次、正式重复 {cfg['repeats']} 次，以种子 {cfg['seed']} 交错执行。全部 {len(data['samples'])} 次运行均通过精确行数和逐列 SHA-256 校验，不受批次边界影响，不使用浮点容差。"),'',
          tr('## Completion time','## 完成耗时'),'']
        for name,f in files.items():
            lines += ['### '+name,'',tr('| Actual selectivity | Rows | DACP/TLS s | FTPS s | FTP s | FTPS/DACP | FTP/DACP |','| 实际命中率 | 行数 | DACP/TLS 秒 | FTPS 秒 | FTP 秒 | FTPS/DACP | FTP/DACP |'),'|---:|---:|---:|---:|---:|---:|---:|']
            for t in THRESHOLDS:
                n=f['expected'][str(t)]['rows'];d=med(name,t,'dacp-tls')
                lines += [f"| {n/f['rows']*100:.3f}% | {n:,} | "+' | '.join(f'{med(name,t,m):.4f}' for m in METHODS)+f" | {med(name,t,'ftps')/d:.2f}× | {med(name,t,'ftp-plain')/d:.2f}× |"]
            lines += ['']
        lines += [tr('Values are medians. Ratios above 1 favor DACP, below 1 favor the FTP method. Chart whiskers show min–max, not confidence intervals. Three repeats are insufficient for a statistical-significance claim.','表中为中位数，耗时比大于 1 表示 DACP 较快，小于 1 表示 FTP 方法较快。图中误差线为最小—最大值，不是置信区间；三次重复不足以支持统计显著性结论。'),'']
        for image,label in [('wall',tr('Completion time','完成耗时')),('first-result',tr('Time to first matching batch','首批匹配结果时间')),('client-cpu',tr('Client CPU time','客户端 CPU 时间'))]:lines += [f'![{label}]({image}{suffix}.svg)','']
        biggest=max(files,key=lambda k:files[k]['bytes']);f=files[biggest]
        lines += [tr('## Result size and interpretation','## 结果大小与解释'),'',tr('| Actual selectivity | FTP source CSV MiB | Matching Arrow logical MiB |','| 实际命中率 | FTP 源 CSV MiB | 匹配记录 Arrow 逻辑 MiB |'),'|---:|---:|---:|']
        for t in THRESHOLDS:lines += [f"| {f['expected'][str(t)]['rows']/f['rows']*100:.3f}% | {f['bytes']/1048576:.3f} | {med(biggest,t,'dacp-tls','result_arrow_bytes')/1048576:.3f} |"]
        lines += ['',tr('The table uses the larger CSV. Arrow logical memory is not IPC/TLS/TCP wire size and cannot establish an exact traffic-saving ratio. FTP reads the entire source even at low selectivity. DACP returns only matches but still incurs server CSV scanning, parsing, filtering and Arrow encoding. High-selectivity or fast-link cases may favor FTP; see the measured table rather than assuming a winner.','上表为较大 CSV 的结果。Arrow 逻辑内存量不是 IPC/TLS/TCP 线速字节，不能据此计算精确网络节省比例。FTP 在低命中率下仍读取全部源文件；DACP 仅返回匹配记录，但仍有服务端扫描、解析、筛选及 Arrow 编码成本。高命中率或高速链路可能有利于 FTP，应以实测表格为准。'),'',
          tr('## Measurement boundaries','## 测量边界'),'',
          tr('- Connection readiness/TLS handshakes and FTP login/TYPE I are outside timing. The DACP client explicitly waits for availability; per-item metadata requests remain inside timing.\n- Reads, parse/filter, transfer, Arrow decoding, all-result consumption and per-column validation are timed. First-result time means the first nonempty matching batch, not first TCP byte.\n- FTP parses directly from the data socket with 64 KiB CSV blocks and a single parser thread; there is no full-download-to-disk delay. FTPS protects control and data connections. Plain FTP is a reference with weaker transport security.\n- Files and server startup are prepared outside timing. OS caches are not cleared; methods are interleaved and run one at a time. Warmups do not guarantee a fully steady-state JVM.\n- Client CPU/RSS are process-wide, sampled every 50 ms. Remote server CPU/RSS are not collected and remain null; remote PIDs are never sampled as local processes.\n- Repetitions, machine differences, application load, RTT, batching and parser differences all limit generalization. This tests a server-side filtering capability, not file transfer speed in isolation.',
             '- 连接就绪、TLS 握手、FTP 登录和 TYPE I 在计时外；DACP 客户端显式等待可用，逐 Item 元数据请求仍在计时内。\n- 读取、解析筛选、传输、Arrow 解码、全部结果消费及逐列校验均在计时内。首批时间指首个非空匹配批次，不是首个 TCP 字节。\n- FTP 直接从数据 socket 流式解析，CSV 块大小 64 KiB，单解析线程，没有先全量下载落盘的额外等待。FTPS 控制和数据连接均加密；明文 FTP 作为安全条件较弱的参考。\n- 数据准备和服务启动不计时；不清理操作系统缓存，各方法交错且串行运行。预热不保证 JVM 完全稳态。\n- 客户端 CPU/RSS 为进程整体统计，每 50 ms 采样。未采集远程服务端 CPU/RSS，相关字段保留 null，不将远程 PID 当成本地进程采样。\n- 重复次数、硬件差异、其他应用负载、RTT、批次和解析器实现都限制结果外推。本测试比较服务端筛选能力，而非孤立的文件传输速度。'),'',
          tr('## Reproduction and raw measurements','## 复现与原始测量'),'',
          '```bash','/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/filter_suite.py \\',
          '  --remote-config .benchmark-runs/csv-filter-wan-staging/remote-config.json \\',
          '  --output .benchmark-runs/csv-filter-wan-20261108 \\',
          '  --profile final --warmups 1 --repeats 3',
          '/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish_filter_wan.py \\',
          '  .benchmark-runs/csv-filter-wan-20261108/results.json \\',
          '  --output .benchmark-runs/csv-filter-wan-20261108/report','```','',
          tr('[All measured and warmup samples](results.json) include fingerprints and validation results. Deployment instructions are in `benchmarks/transfer/README.md`. Reports are generated only in the explicitly selected directory; website/document publication is a separate action.','[全部正式及预热样本](results.json)包含版本指纹及校验结果。部署步骤见 `benchmarks/transfer/README_zh.md`。报告只生成到显式指定目录，不自动发布到官网或说明文档。')]
        (output/('report'+suffix+'.md')).write_text('\n'.join(lines)+'\n')

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('results',type=Path);p.add_argument('--output',required=True,type=Path)
    a=p.parse_args();publish(a.results,a.output)
