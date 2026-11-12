#!/usr/bin/env python3
"""Publish reproducible CSV filtering results as bilingual reports and SVGs."""
import argparse
import json
from pathlib import Path
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from suite import ROOT, METHODS

LABELS={'dacp-tls':'DACP/TLS','ftps':'FTPS','ftp-plain':'FTP'}
COLORS=['#087e8b','#e4a23b','#788ca4']

def validate(data):
    cfg=data['config']
    if cfg.get('profile')!='final':raise ValueError('A final-profile batch is required')
    samples=data['samples'];files=data['fixtures'];thresholds=[10,100,1000,5000,10000]
    expected=len(files)*len(thresholds)*3*(cfg['repeats']+cfg['warmups'])
    if len(samples)!=expected or any(s['state']!='succeeded' for s in samples):
        raise ValueError('Refuse incomplete or failed measurements')
    for name,f in files.items():
        for t in thresholds:
            for method in METHODS:
                group=[s for s in samples if s['file']==name and s['threshold']==t and s['method']==method]
                if sorted(s['repeat'] for s in group)!=list(range(-cfg['warmups'],cfg['repeats'])):
                    raise ValueError('Missing/duplicate trials')
                for s in group:
                    e=f['expected'][str(t)]
                    if s['rows']!=e['rows'] or s['sha256']!=e['sha256']:raise ValueError('Unverified results')
    return data

def publish(source):
    data=validate(json.loads(source.read_text()));cfg=data['config'];samples=data['samples'];files=data['fixtures'];thresholds=[10,100,1000,5000,10000]
    output=ROOT/'docs/benchmarks/csv-filter';output.mkdir(parents=True,exist_ok=True)
    (output/'results.json').write_text(json.dumps(data,indent=2)+'\n')
    def values(name,t,method,field):
        return [s[field] for s in samples if s['file']==name and s['threshold']==t and s['method']==method and not s['warmup']]
    def med(name,t,m,f='wall_s'):return statistics.median(values(name,t,m,f))
    metrics=[('wall','wall_s','End-to-end time (s)','端到端耗时（秒）'),
             ('first-result','first_result_s','First matching batch (s)','首批匹配结果（秒）'),
             ('client-cpu','client_cpu_s','Client CPU time (s)','客户端 CPU 时间（秒）'),
             ('server-cpu','server_cpu_s','Server process-tree CPU time (s)','服务端进程树 CPU 时间（秒）')]
    for zh in [False,True]:
        suffix='_zh' if zh else ''
        plt.rcParams['font.family']=('WenQuanYi Zen Hei' if zh else 'DejaVu Sans')
        plt.rcParams['svg.fonttype']='path'
        for image,field,en,cn in metrics:
            fig,axes=plt.subplots(1,len(files),figsize=(12,4),squeeze=False)
            for ax,(name,f) in zip(axes[0],files.items()):
                for i,m in enumerate(METHODS):
                    ys=[med(name,t,m,field) for t in thresholds]
                    lo=[y-min(values(name,t,m,field)) for y,t in zip(ys,thresholds)]
                    hi=[max(values(name,t,m,field))-y for y,t in zip(ys,thresholds)]
                    ax.bar([j+(i-1)*.24 for j in range(5)],ys,.23,color=COLORS[i],label=LABELS[m],yerr=[lo,hi],capsize=2)
                ax.set_xticks(range(5),['0.1%','1%','10%','50%','100%'])
                ax.set_xlabel('阈值比例（实际命中率见表）' if zh else 'Bucket range fraction (actual selectivity in table)')
                ax.set_ylabel(cn if zh else en);ax.set_title(f"{f['rows']:,} {'行' if zh else 'rows'} · {f['bytes']/1048576:.1f} MiB")
                ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
            axes[0][0].legend();fig.tight_layout();fig.savefig(output/(image+suffix+'.svg'));fig.savefig('/tmp/csv-filter-'+image+suffix+'.png',dpi=130);plt.close(fig)
        lines=[('# 远程 CSV 属性过滤：DACP 与 FTP/FTPS 实测' if zh else '# Remote CSV filtering: measured DACP versus FTP/FTPS'),'',('[English](csv-filter-benchmark.md)' if zh else '[中文](csv-filter-benchmark_zh.md)'),'',data['timestamp'],'',
        ('**本机 IPv4 回环测试，不代表公网或跨中心性能。测试比较完整筛选任务，计算位置及解析实现不同，不能把差异全部归因于协议。**' if zh else '**IPv4-loopback measurements on one host, not WAN or cross-center performance. This compares complete filtering tasks with different computation placement and parser implementations; differences cannot be attributed entirely to protocols.**'),'',
        ('## 执行路径' if zh else '## Execution paths'),'','```mermaid','flowchart LR',
        ('    CSV[同一份 CSV] --> FTP[FTP / FTPS：全量传输]' if zh else '    CSV[Same CSV] --> FTP[FTP / FTPS: full transfer]'),
        ('    FTP --> Local[客户端 PyArrow 流式解析与过滤]' if zh else '    FTP --> Local[Client PyArrow streaming parse and filter]'),
        ('    CSV --> DACP[DACP：服务端扫描与过滤]' if zh else '    CSV --> DACP[DACP: server scan and filter]'),
        ('    DACP --> Arrow[仅返回匹配记录的 Arrow 流]' if zh else '    DACP --> Arrow[Arrow stream of matching records]'),
        ('    Local --> Check[相同行数与逐列 SHA-256 校验]' if zh else '    Local --> Check[Identical row-count and per-column SHA-256 checks]'),
        '    Arrow --> Check','```','',('## 数据与方法' if zh else '## Data and method'),'']
        for name,f in files.items():lines += [f"- `{name}`: {f['rows']:,} rows, {f['bytes']:,} bytes ({f['bytes']/1048576:.2f} MiB); SHA-256 `{f['sha256']}`."]
        lines += ['',('字段为 `id:int64`、`bucket:int64`、`quality:int64`、`payload:string`。CSV 保存十进制数值，双方按上述类型读取。`bucket = id % 10000`；payload 为确定性的 128 字符字符串。不同数据集的实际命中率按实测行数列出。' if zh else 'Fields are `id:int64`, `bucket:int64`, `quality:int64`, `payload:string`. CSV stores decimal numbers; both paths read these types. `bucket = id % 10000`; payload is a deterministic 128-character string. Actual selectivity is calculated from verified result counts.'),'',
        '```sql','SELECT * FROM source WHERE bucket <= :threshold','```','',
        ('阈值为 9、99、999、4999、9999。DACP 使用 SDK 的等价类型化谓词；FTP/FTPS 使用 PyArrow 的等价比较。所有列均返回，不投影、不限行。' if zh else 'Thresholds are 9, 99, 999, 4999 and 9999. DACP uses the equivalent typed SDK predicate; FTP/FTPS use the equivalent PyArrow comparison. All columns are returned, with no projection or row limit.'),'',
        (f"每组预热 {cfg['warmups']} 次、正式重复 {cfg['repeats']} 次，按固定随机种子交错执行，共 {len(samples)} 次，全部通过行数及逐列内容摘要校验。" if zh else f"Each combination has {cfg['warmups']} warmups and {cfg['repeats']} measured repeats, interleaved with a fixed random seed: {len(samples)} trials, all passing row-count and per-column content checks."),'',
        ('- 不主动清理操作系统页缓存；正式结果按热缓存测试解释，预热不能保证 JVM 达到完全稳态。\n- 每次连接建立及 TLS 握手在计时外；正常元数据请求、CSV 读取解析、过滤、传输、Arrow 解码和校验在计时内。FTP 登录及 TYPE I 同样在计时外。\n- FTP/FTPS 直接从数据 socket 流式解析，64 KiB 读取块、单解析线程，无完整下载后落盘步骤；DACP 使用默认服务端记录批次。双方均不落结果文件。\n- DACP/TLS 与 FTPS 验证同一临时证书及主机名；FTPS 的控制、数据通道均加密。数据集公开，两边均匿名访问；明文 FTP 仅供参考。\n- 完成时间包含全部结果的消费及校验；首批时间为首个非空过滤结果批次到达，受批大小影响，不等于网络首字节。\n- CPU、RSS 按 50 ms 采样；服务端为近似进程树统计，短 FTP 请求的 CPU 可能低于计时分辨率。RSS 是进程总量，不是单请求增量。' if zh else '- OS page caches are not explicitly cleared; interpret measured results as warm-cache tests. Warmups do not guarantee a fully steady-state JVM.\n- Connection setup and TLS handshakes are outside timing for each trial; normal metadata requests, CSV read/parse/filter, transfer, Arrow decode and validation are timed. FTP login and TYPE I are also outside timing.\n- FTP/FTPS parse directly from the data socket with 64 KiB blocks and one parser thread, without a download-to-disk phase; DACP uses default server record batches. Neither path saves result files.\n- DACP/TLS and FTPS validate the same temporary certificate and hostname; FTPS protects both channels. Datasets are public and both paths are anonymous. Plain FTP is a reference only.\n- Completion includes consuming and validating all results; first-result time measures the first nonempty matching batch, depends on batching and is not time to first network byte.\n- CPU/RSS are sampled every 50 ms; server figures approximate the process tree, and short FTP CPU times may fall below resolution. RSS is total process memory, not incremental request memory.'),'',
        ('## 环境' if zh else '## Environment'),'',f"- {data['host']['platform']}; {data['host']['logical_cpus']} logical CPUs.",f"- Python {data['python'].split()[0]}; PyArrow {data['pyarrow']}; vsftpd {data['runtime_inventory']['vsftpd_version']}.",'',
        ('## 完成耗时' if zh else '## Completion time'),'']
        for name,f in files.items():
            lines += ['### '+name,'',('| 实际命中率 | 结果行数 | DACP/TLS 秒 | FTPS 秒 | FTP 秒 | FTPS / DACP 耗时比 |' if zh else '| Actual selectivity | Result rows | DACP/TLS s | FTPS s | FTP s | FTPS / DACP time ratio |'),'|---:|---:|---:|---:|---:|---:|']
            for t in thresholds:
                count=f['expected'][str(t)]['rows'];a=med(name,t,'dacp-tls');b=med(name,t,'ftps')
                lines += [f"| {100*count/f['rows']:.3f}% | {count:,} | "+' | '.join(f'{med(name,t,m):.4f}' for m in METHODS)+f' | {b/a:.2f}× |']
            lines += ['']
        lines += [('大于 1 表示 DACP 较快，小于 1 表示 FTPS 较快。数值为中位数；图中误差线为最小至最大值，不是置信区间。' if zh else 'A ratio above 1 means DACP is faster; below 1 means FTPS is faster. Values are medians; figure whiskers span min–max, not confidence intervals.'),'']
        for image,field,en,cn in metrics:lines += [f'![{cn if zh else en}](benchmarks/csv-filter/{image}{suffix}.svg)','']
        lines += [('## 结果解释与适用范围' if zh else '## Interpretation and scope'),'']
        biggest=max(files,key=lambda n:files[n]['bytes'])
        for t in [10,10000]:
            a=med(biggest,t,'dacp-tls');b=med(biggest,t,'ftps')
            lines += [(f"- `{biggest}`，阈值 {t-1}：DACP {a:.4f} 秒，FTPS {b:.4f} 秒；FTPS/DACP 耗时比 {b/a:.2f}×。" if zh else f"- `{biggest}`, threshold {t-1}: DACP {a:.4f} s, FTPS {b:.4f} s; FTPS/DACP time ratio {b/a:.2f}×.")]
        lines += ['',('DACP 减少传回客户端的记录，但普通 CSV 仍需服务端完整扫描；此测试没有索引，也没有测量受限带宽或 WAN。FTP 传输完整 CSV。`result_arrow_bytes` 是匹配批次的 Arrow 逻辑内存字节，不是网络线速字节；原始 JSON 同时保留 FTP 下载大小。不能以两者之差声称测得实际网络流量节省。' if zh else 'DACP reduces records returned to the client, but ordinary CSV still requires a complete server scan. There is no index, constrained-bandwidth test or WAN measurement here. FTP transfers the complete CSV. `result_arrow_bytes` counts logical Arrow batch memory, not wire bytes; raw JSON also retains FTP download size. Their difference is not a measurement of actual wire-traffic savings.'),'',
        ('不同实现的 CPU 成本、批次与本机高速回环都会影响结果。低命中率、带宽较低时的潜在优势需单独实测，不能从本结果外推；同样不能因为本机结果较慢而推断所有远程查询都较慢。' if zh else 'Parser CPU costs, batching and fast loopback affect results. Potential benefits at low selectivity on slower links require separate measurement and cannot be inferred from these timings; slower local results likewise do not establish that all remote queries are slower.'),'',
        ('## 复现与原始数据' if zh else '## Reproduction and raw data'),'','```bash',
        '/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/filter_suite.py \\',
        '  --output "$PWD/.benchmark-runs/csv-filter-final" \\',
        '  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \\',
        '  --profile final --warmups 1 --repeats 3',
        '/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish_filter.py \\',
        '  .benchmark-runs/csv-filter-final/results.json','```','',
        ('安装和权限要求见[测试工具说明](../benchmarks/transfer/README_zh.md)。[原始测量 JSON](benchmarks/csv-filter/results.json)包含所有正式与预热样本、摘要、资源信息和服务端/客户端版本指纹，不含凭据。测试数据、私钥与临时配置保留在忽略的运行目录，不应提交。' if zh else 'See [harness setup](../benchmarks/transfer/README.md) for installation and permissions. [Raw measurement JSON](benchmarks/csv-filter/results.json) includes measured and warmup samples, digests, resources and server/client artifact fingerprints, without credentials. Source data, private keys and temporary configurations remain in the ignored run directory and must not be committed.')]
        (ROOT/'docs'/('csv-filter-benchmark'+suffix+'.md')).write_text('\n'.join(lines)+'\n')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('results',type=Path);publish(p.parse_args().results)
