#!/usr/bin/env python3
"""Generate bilingual transfer reports and SVG figures from a complete measured batch."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import statistics
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy as np

ROOT=Path(__file__).resolve().parents[2]
METHODS=['dacp-tls','ftps','ftp-plain']
COLORS=['#0891b2','#8b5cf6','#64748b']
LABELS={'large-100MiB':'100 MiB','large-1GiB':'1 GiB','small-batch':'1000 × 64 KiB','parallel-4':'4 × 100 MiB','parallel-8':'8 × 100 MiB','large-100MiB-fresh':'100 MiB\nfresh','small-batch-fresh':'1000 small\nfresh'}

def publish(source,destination):
    data=json.loads(source.read_text());cfg=data['config']
    expected=len(data['cases'])*2*len(METHODS)*(cfg['warmups']+cfg['repeats'])
    if cfg['profile']!='final' or len(data['samples'])!=expected:raise ValueError('A complete final-profile batch is required')
    required={(c['name'],sink,method,repeat) for c in data['cases'] for sink in ['consume','disk-fsync'] for method in METHODS for repeat in range(-cfg['warmups'],cfg['repeats'])}
    actual=[(s['case'],s['sink'],s['method'],s['repeat']) for s in data['samples']]
    if len(set(actual))!=len(actual) or set(actual)!=required:
        raise ValueError('Missing or duplicate trial coverage')
    groups={}
    for s in data['samples']:
        if not s['warmup'] and s['state']=='succeeded':groups.setdefault((s['case'],s['sink'],s['method']),[]).append(s)
    def values(case,sink,method,field):return [s[field] for s in groups.get((case,sink,method),[]) if s.get(field) is not None]
    def median(case,sink,method,field):
        v=values(case,sink,method,field);return statistics.median(v) if v else None
    def fmt(value,precision=3):return '—' if value is None else f'{value:.{precision}f}'
    destination.mkdir(parents=True,exist_ok=True);assets=destination/'benchmarks/transfer';assets.mkdir(parents=True,exist_ok=True)
    clean=copy.deepcopy(data)
    if 'runtime_inventory' not in clean:
        clean['runtime_inventory']=json.loads((source.parent/'runtime-inventory.json').read_text())
    clean['benchmark_source_sha256']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(__file__).parent.glob('*.py'))}
    clean['client_source_sha256']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT/'dacp-client/dacp').glob('*.py'))}
    clean['server_artifact_sha256']={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'dacp-server/target/universal/stage/lib').glob('*dacp-server*.jar')}
    raw_name='transfer-'+data['timestamp'][:10].replace('-','')+'.json';(assets/raw_name).write_text(json.dumps(clean,indent=2)+'\n')
    cases=[c['name'] for c in data['cases']];failures=sum(s['state']!='succeeded' for s in data['samples'])
    metrics=[('throughput','MiB_s','Effective payload throughput','有效负载吞吐','MiB/s',1),('wall','wall_s','End-to-end wall time','端到端总耗时','s',1),('first-byte','first_byte_s','Time to first payload byte','首字节延迟','ms',1000),('client-cpu','client_cpu_s','Client CPU time (all worker threads)','客户端 CPU 时间（全部工作线程）','CPU s',1),('server-cpu','server_cpu_s_approx','Approximate server process-tree CPU time','服务端进程树 CPU 时间（近似）','CPU s',1),('server-rss','server_peak_rss_bytes','Sampled server process-tree peak RSS','服务端进程树 RSS 峰值（采样）','MiB',1/1048576),('client-rss','client_peak_rss_bytes','Sampled client process peak RSS','客户端进程 RSS 峰值（采样）','MiB',1/1048576)]
    for zh in (False,True):
        suffix='_zh' if zh else '';font=FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc') if zh else FontProperties()
        labels=['DACP / TLS','FTPS','FTP 明文参考'] if zh else ['DACP / TLS','FTPS','Plain FTP reference']
        for name,field,en,cn,unit,scale in metrics:
            fig,axes=plt.subplots(2,1,figsize=(14,9))
            for ax,sink in zip(axes,['consume','disk-fsync']):
                width=.24
                for i,method in enumerate(METHODS):
                    for j,case in enumerate(cases):
                        v=[x*scale for x in values(case,sink,method,field)]
                        if not v:continue
                        m=statistics.median(v);x=j+(i-1)*width
                        ax.bar(x,m,width,color=COLORS[i],label=labels[i] if j==0 else None)
                        ax.errorbar(x,m,yerr=[[m-min(v)],[max(v)-m]],fmt='none',ecolor='#334155',capsize=3)
                        ax.annotate(f'{m:.1f}',(x,max(v)),xytext=(0,4),textcoords='offset points',ha='center',fontsize=7)
                ax.set_xticks(np.arange(len(cases)),[LABELS[c] for c in cases],fontsize=9)
                ax.set_ylabel(unit);ax.set_title(('直接消费并校验' if sink=='consume' else '落盘、校验并 fsync') if zh else ('Consume and verify' if sink=='consume' else 'Write, verify and fsync'),fontproperties=font)
                ax.grid(axis='y',alpha=.15);ax.set_axisbelow(True);ax.spines[['top','right']].set_visible(False);ax.legend(prop=font,loc='upper left',bbox_to_anchor=(1.01,1))
                ax.margins(y=.15)
            fig.suptitle(cn if zh else en,fontproperties=font,fontsize=14);fig.tight_layout();fig.savefig(assets/(name+suffix+'.svg'),bbox_inches='tight');plt.close(fig)
        lines=['# '+('DACP 与 FTP 文件传输实测' if zh else 'Measured DACP and FTP file transfers'),'',data['timestamp'],'']
        if zh:
            lines += [f'本机 IPv4 回环测试，三个独立服务进程读取同一批原始文件：DACP/TLS、vsftpd FTPS 和未加密 FTP 参考。共 {expected} 次运行（含预热），失败 {failures} 次。每个组合预热 {cfg["warmups"]} 次、正式重复 {cfg["repeats"]} 次。所有成功传输逐文件校验字节数及 SHA-256。', '', '**这些结果衡量同一主机上的协议及实现开销，不代表物理网络吞吐或广域网表现。主对比是 DACP/TLS 与 FTPS，明文 FTP 的安全条件不同，仅供参考。**', '', '## 测试拓扑', '']
        else:
            lines += [f'An IPv4-loopback benchmark uses three isolated server processes serving identical raw files: DACP/TLS, vsftpd FTPS and plain FTP as a reference. There are {expected} runs including warmups, with {failures} failures. Each combination has {cfg["warmups"]} warmups and {cfg["repeats"]} measured repeats. Every successful file passes byte-count and SHA-256 validation.', '', '**This measures protocol and implementation overhead on one host, not physical-network throughput or WAN performance. The primary comparison is DACP/TLS versus FTPS; unencrypted FTP has different security and is only a reference.**', '', '## Test topology', '']
        lines+=['```mermaid','flowchart LR','  Files[Same raw files] --> D[DACP / TLS]','  Files --> S[vsftpd / FTPS]','  Files --> P[vsftpd / plain FTP]','  D --> SDK[Python DACP SDK]','  S --> Curl[Python + libcurl]','  P --> Curl','  SDK --> Sink[Same byte count and SHA-256 receiver]','  Curl --> Sink','  Sink --> Memory[Consume without a payload file]','  Sink --> Disk[Write and fsync each file]','```','']
        if zh:
            lines += ['## 工作负载与计时','', '- 大文件：100 MiB 和 1 GiB；小文件：1,000 个 64 KiB 文件。4/8 路并发各自下载同一个 100 MiB 源文件，总负载为 400/800 MiB。', '- 连接复用：每个工作线程持有一个独立客户端，会先完成连接及首个 Item/文件的元信息请求，准备不计时；随后保留正常逐文件元信息请求。新连接组为每次试验新建客户端，不是每个小文件重新连接。', '- 所有方法使用相同生成内容、相同 64 KiB 接收目标分块、相同 SHA-256 校验。libcurl 回调实际分块可能更小；DACP 包含 SDK 和 Arrow 解码开销。文件为实际写入的确定性伪随机数据，不是稀疏文件。', '- 直接消费组不保存接收负载；落盘组逐文件写入、flush、fsync 和 close 均计时。未对目录 fsync，不评价断电恢复。临时接收文件在计时后删除。', '- 服务启动、证书及账号生成、源文件生成、连接复用准备不计时。JVM/系统文件缓存保留，方法按固定种子随机交错；不强制清空缓存。线程池启动、元信息请求、传输、解码、校验及资源采样均计时。', '- 两种 TLS 方法校验同一临时自签名证书及主机名。DACP 使用 reader Token，FTP/FTPS 使用只读匿名身份；因此不是只替换传输介质的消融实验。FTPS 控制和数据通道都强制 TLS，libcurl 开启 TLS 会话缓存；vsftpd 不强制会话复用。无显式压缩。', '', '## 环境','']
        else:
            lines += ['## Workloads and timing','', '- Large files: 100 MiB and 1 GiB. Small files: 1,000 × 64 KiB. Four/eight concurrent downloads read the same 100 MiB source, for 400/800 MiB aggregate payloads.', '- Reused connections: one independent client per worker, prepared with connection and first Item/file metadata outside timing; normal per-file metadata requests remain timed. Fresh cases create clients per trial, not per small file.', '- Identical generated contents, 64 KiB receive targets and SHA-256 checks are used. Actual libcurl callback blocks may be smaller. DACP includes SDK and Arrow decoding overhead. Files contain physically written deterministic pseudorandom data, not sparse holes.', '- Consume mode saves no received payload. Disk mode times writing, flush, fsync and close for every file. Directories are not fsynced and power-loss recovery is not evaluated. Received files are removed after timing.', '- Startup, certificate/account creation, input generation and reused-connection preparation are excluded. JVM/OS caches remain warm; methods are randomly interleaved using a fixed seed. Worker-pool startup, metadata requests, transfers, decoding, validation and resource sampling are timed.', '- TLS methods validate the same temporary self-signed certificate and hostname. DACP uses a reader Token; FTP/FTPS use read-only anonymous identities, so this is not a transport-only ablation. FTPS requires TLS on control and data channels. libcurl TLS session caching is enabled; vsftpd does not require session reuse. No explicit compression is enabled.', '', '## Environment','']
        lines += [f'- {data["host"]["platform"]}; {data["host"]["logical_cpus"]} logical CPUs; {data["host"]["memory_bytes"]/1073741824:.1f} GiB RAM.',f"- Python {data['python'].split()[0]}; PyArrow {data['versions']['pyarrow']}; vsftpd {clean['runtime_inventory']['vsftpd_version']}.",f'- {data["curl"]}.',f'- DACP byte_chunk: 65,536; seed: {cfg["seed"]}.','', '## '+('正式运行耗时' if zh else 'Measured wall time'),'']
        for sink in ['consume','disk-fsync']:
            lines += ['### '+(('直接消费' if sink=='consume' else '落盘并 fsync') if zh else sink),'',('| 场景 | DACP/TLS 秒 | FTPS 秒 | 明文 FTP 秒 | DACP/FTPS 耗时比 |' if zh else '| Case | DACP/TLS s | FTPS s | Plain FTP s | DACP/FTPS time ratio |'),'|---|---:|---:|---:|---:|']
            for case in cases:
                a=median(case,sink,'dacp-tls','wall_s');b=median(case,sink,'ftps','wall_s')
                lines.append('| '+case+' | '+' | '.join(fmt(median(case,sink,m,'wall_s')) for m in METHODS)+' | '+('—' if a is None or not b else f'{a/b:.2f}×')+' |')
            lines += ['']
        lines += ['## '+('主要观察' if zh else 'Main observations'),'']
        for case in ['large-1GiB','small-batch','parallel-4','parallel-8']:
            a=median(case,'consume','dacp-tls','MiB_s');b=median(case,'consume','ftps','MiB_s')
            if a is not None and b:
                lines.append((f'- `{case}` 直接消费：DACP/TLS {a:.1f} MiB/s，FTPS {b:.1f} MiB/s，吞吐比 {a/b:.2f}×。' if zh else f'- `{case}`, consume: DACP/TLS {a:.1f} MiB/s, FTPS {b:.1f} MiB/s; throughput ratio {a/b:.2f}×.'))
        lines += [('- 逐文件 fsync 会引入存储延迟与波动，不能据此把落盘差异全部解释为传输协议差异。' if zh else '- Per-file fsync introduces storage latency and variability; disk-mode differences cannot be assigned entirely to transport protocols.'), '']
        lines += [('耗时比大于 1 表示 DACP 较慢，小于 1 表示 DACP 较快。数值为成功正式样本的中位数。' if zh else 'A time ratio above 1 means DACP is slower; below 1 means faster. Values are medians of successful measured trials.'),'','## '+('实测图表' if zh else 'Measured figures'),'',('柱为中位数，误差线为最小到最大值，不是置信区间。吞吐只按有效负载计算，不含 TLS、TCP、FTP 或 Arrow 封装字节。每张图分别展示直接消费及落盘组。' if zh else 'Bars show medians; whiskers span minimum to maximum, not confidence intervals. Throughput counts useful payload bytes, excluding TLS/TCP/FTP/Arrow framing. Each figure separates consume and disk cases.'),'']
        for name,field,en,cn,unit,scale in metrics:lines += [f'![{cn if zh else en}](benchmarks/transfer/{name}{suffix}.svg)','']
        lines += ['## '+('成功次数' if zh else 'Completion counts'),'','| '+('方法 | 成功 / 全部（含预热）' if zh else 'Method | Succeeded / all (including warmups)')+' |','|---|---:|']
        for method in METHODS:
            ss=[s for s in data['samples'] if s['method']==method];lines.append(f'| {method} | {sum(s["state"]=="succeeded" for s in ss)}/{len(ss)} |')
        lines += ['','## '+('解释与限制' if zh else 'Interpretation and limits'),'']
        if zh:
            lines += ['- 这些结果比较当前服务端和客户端实现，不能证明某协议在所有实现或网络上始终更快。所有协议均在同机读取同一文件，并共享 CPU、内存和存储。', '- SHA-256 与接收循环可能限制回环吞吐。不能把这里的速度当作链路带宽，亦不能把差异全部归因于 Arrow、TLS 或某一层。', '- 服务端 CPU 为进程树计数的近似差值，包含已回收子进程；短命进程和回收时序可能影响精度。客户端 CPU 为整个测试进程累计时间，包含工作线程和资源采样。', '- RSS 每 50 ms 采样：服务端值为进程树 RSS 之和，不去除共享页，不是 PSS，也不是每次请求新增内存。客户端含 SDK/libcurl 已加载库及分配器缓存，受试验顺序影响。短暂峰值可能漏采。', '- 只有三个正式重复，不能据此给出稳定的生产容量或尾延迟结论。失败样本保留在原始记录中，不计入成功中位数。', '- 不测试跨机器、广域网、上传、查询下推、结构化视图、Job 执行、故障恢复或压缩。现有生产服务没有参与测试。', '', '## 复现与原始记录','', '- [测试程序与运行说明](../benchmarks/transfer/README_zh.md)',f'- [全部原始记录](benchmarks/transfer/{raw_name})', '- 原始记录包含服务产物、vsftpd 二进制和 DACP SDK 的 SHA-256；benchmark_source_sha256 为生成报告时的测试源码快照。']
        else:
            lines += ['- Results compare concrete server/client implementations, not a universal protocol ranking. All methods read the same files on one shared host, competing for CPU, memory and storage.', '- SHA-256 and receive loops may limit loopback throughput. These rates are not link capacity; differences cannot be attributed entirely to Arrow, TLS or any single layer.', '- Server CPU is an approximate process-tree counter delta including reaped children; short-lived processes and reaping timing affect precision. Client CPU covers the entire benchmark process, workers and resource sampler.', '- RSS is sampled every 50 ms. Server values sum process RSS without deduplicating shared pages: they are not PSS or per-request incremental memory. Client RSS includes loaded SDK/libcurl libraries and allocator caches and depends on trial order. Short peaks may be missed.', '- Three measured repeats do not establish production capacity or tail latency. Failed trials remain in raw data and are excluded from successful medians.', '- Cross-host/WAN transport, uploads, query pushdown, structured views, Jobs, recovery and compression are not tested. Production services do not participate.', '', '## Reproduction and raw records','', '- [Programs and commands](../benchmarks/transfer/README.md)',f'- [All raw records](benchmarks/transfer/{raw_name})', '- Raw records include SHA-256 identities for the server artifact, vsftpd binary and DACP SDK; benchmark_source_sha256 identifies benchmark sources at report generation.']
        lines += ['', '- [libcurl TLS control/data protection](https://curl.se/libcurl/c/CURLOPT_USE_SSL.html)', '- [vsftpd configuration](https://security.appspot.com/vsftpd/vsftpd_conf.html)']
        if zh:
            translations={'Same raw files':'同一批原始文件','Python DACP SDK':'Python DACP SDK','Python + libcurl':'Python + libcurl','Same byte count and SHA-256 receiver':'统一字节计数及 SHA-256 校验','Consume without a payload file':'直接消费，不保存接收负载','Write and fsync each file':'逐文件写入并 fsync'}
            for i,line in enumerate(lines):
                for a,b in translations.items():line=line.replace(a,b)
                lines[i]=line
        (destination/('transfer-benchmark'+suffix+'.md')).write_text('\n'.join(lines)+'\n')
    return failures

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('results',type=Path);p.add_argument('--docs',type=Path,default=ROOT/'docs');a=p.parse_args()
    failures=publish(a.results,a.docs);print('Published bilingual reports and figures; failed trials:',failures);sys.exit(bool(failures))
