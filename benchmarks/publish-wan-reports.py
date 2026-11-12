#!/usr/bin/env python3
"""Publish verified WAN workflow/filter reports with bundled, independent assets."""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--check-dams-results',type=Path,required=True)
p.add_argument('--csv-filter-results',type=Path,required=True)
p.add_argument('--conet-home',type=Path,required=True,help='Authoring-time source of the check-dam report generator')
a=p.parse_args()
for path,count in [(a.check_dams_results,8),(a.csv_filter_results,120)]:
    data=json.loads(path.read_text())
    if not data.get('remote_environment') or len(data['samples'])!=count:raise ValueError('Expected the complete remote measurement batch')
with tempfile.TemporaryDirectory(prefix='dacp-wan-reports-') as temporary:
    temp=Path(temporary)
    for slug,script,source in [
        ('check-dams',a.conet_home/'benchmarks/check-dams/publish.py',a.check_dams_results),
        ('csv-filter',ROOT/'benchmarks/transfer/publish_filter_wan.py',a.csv_filter_results)]:
        staged=temp/slug
        subprocess.run([sys.executable,str(script.resolve()),str(source.resolve()),'--output',str(staged)],check=True)
        assets=ROOT/'docs/benchmarks'/(slug+'-wan');assets.mkdir(parents=True,exist_ok=True)
        for asset in staged.rglob('*'):
            if asset.is_file() and asset.suffix in ['.svg','.json']:
                target=assets/asset.relative_to(staged);target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(asset,target)
        for zh in [False,True]:
            suffix='_zh' if zh else ''
            text=(staged/('report'+suffix+'.md')).read_text().replace('The small sample do not','The small sample does not')
            text=text.replace('(report.md)',f'({slug}-benchmark.md)').replace('(report_zh.md)',f'({slug}-benchmark_zh.md)')
            text=text.replace('(results.json)',f'(benchmarks/{slug}-wan/results.json)')
            if slug=='check-dams':
                text=text.replace('(figures/',f'(benchmarks/{slug}-wan/figures/')
                text=text.replace('[执行方法与前提](../README_zh.md) · ','').replace('[Reproduction and prerequisites](../README.md) · ','')
                text += ('\n## 复现\n\n测试程序位于 [CONET 测试工具目录](https://github.com/rdcn-link/conet/tree/main/benchmarks/check-dams)，包括远程隔离服务部署、FTP 下载和相同 Job 的本机执行。数据、镜像及算子预先准备；连接配置仅保存在本地私有目录。以下命令在 CONET 仓库中执行：\n\n' if zh else '\n## Reproduction\n\nThe [CONET benchmark tools](https://github.com/rdcn-link/conet/tree/main/benchmarks/check-dams) provide isolated remote deployment, FTP downloads and local execution of the same Job. Prepare data, images and operators first; keep connection configuration private. Run these commands in the CONET checkout:\n\n')
                text += '```bash\npython benchmarks/check-dams/compare.py \\\n  --remote-config .state/benchmarks/remote-upload/remote-config.json \\\n  --url http://39.106.98.67:28720 \\\n  --output .state/benchmarks/check-dams-wan-verified-20261108 \\\n  --warmups 1 --repeats 3 --timeout 1200\n```\n'
            else:
                text=re.sub(r'\]\(((?:wall|first-result|client-cpu)(?:_zh)?\.svg)\)',rf'](benchmarks/{slug}-wan/\1)',text)
                text=text.replace('Deployment instructions are in `benchmarks/transfer/README.md`.','See [deployment instructions](../benchmarks/transfer/README.md).').replace('部署步骤见 `benchmarks/transfer/README_zh.md`。','参见[部署步骤](../benchmarks/transfer/README_zh.md)。')
            (ROOT/'docs'/(slug+'-benchmark'+suffix+'.md')).write_text(text)
print('Published two bilingual WAN reports, figures and verified measurement JSON')
