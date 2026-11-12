import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock,patch
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import filter_suite as suite
import publish_filter

class FilterContracts(unittest.TestCase):
    def test_fixture_truth_matches_independent_reader_and_numeric_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'data.csv';fixture=suite.generate(path,10001)
            table=csv.read_csv(path,convert_options=csv.ConvertOptions(column_types=suite.SCHEMA))
            self.assertEqual(fixture['sha256'],hashlib.sha256(path.read_bytes()).hexdigest())
            for threshold in suite.THRESHOLDS:
                selected=table.filter(pc.less(table['bucket'],threshold))
                hashes=[hashlib.sha256() for _ in suite.NAMES]
                for batch in selected.to_batches(max_chunksize=137):suite.fingerprint(batch,hashes)
                self.assertEqual(fixture['expected'][str(threshold)],dict(rows=selected.num_rows,sha256=[h.hexdigest() for h in hashes]))
            self.assertEqual(fixture['expected']['10']['rows'],11)
            self.assertEqual(fixture['expected']['10000']['rows'],10001)
    def test_fingerprint_preserves_null_empty_quotes_newlines_and_order(self):
        values=[None,'','a,b','a"b','a\nb','数据']
        table=pa.table([pa.array(values)]*4,names=suite.NAMES)
        def digest(batches):
            hashes=[hashlib.sha256() for _ in suite.NAMES]
            for batch in batches:suite.fingerprint(batch,hashes)
            return [h.hexdigest() for h in hashes]
        self.assertEqual(digest(table.to_batches()),digest(table.to_batches(max_chunksize=1)))
        self.assertNotEqual(digest(table.to_batches()),digest(table.take(pa.array([5,4,3,2,1,0])).to_batches()))
    def test_remote_dacp_uses_public_host_verified_certificate_and_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            ca=Path(directory)/'cert.pem';ca.write_bytes(b'trusted benchmark certificate')
            servers=SimpleNamespace(out=Path(directory),host='39.106.98.67',ca=ca,ports={'dacp-tls':28801})
            source=MagicMock()
            with patch.object(suite.Source,'open',return_value=source) as opened:
                client=suite.Client('dacp-tls',servers)
                opened.assert_called_once_with('dacp://39.106.98.67:28801/bench',tls_root_certs=ca.read_bytes())
                source._client.wait_for_available.assert_called_once_with(timeout=30)
                client.close();source.close.assert_called_once()
    def test_remote_ftps_verifies_certificate_and_protects_data(self):
        with tempfile.TemporaryDirectory() as directory:
            ca=Path(directory)/'cert.pem';servers=SimpleNamespace(out=Path(directory),host='39.106.98.67',ca=ca,ports={'ftps':28802})
            ftp=MagicMock()
            with patch.object(suite.ssl,'create_default_context') as context,patch.object(suite.ftplib,'FTP_TLS',return_value=ftp):
                client=suite.Client('ftps',servers)
                context.assert_called_once_with(cafile=str(ca));ftp.connect.assert_called_once_with('39.106.98.67',28802,timeout=300)
                ftp.prot_p.assert_called_once();ftp.voidcmd.assert_called_once_with('TYPE I');client.close()
    def test_remote_trial_reports_unknown_server_resources_as_null(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'data.csv';fixture=suite.generate(path,10)
            table=csv.read_csv(path,convert_options=csv.ConvertOptions(column_types=suite.SCHEMA))
            client=SimpleNamespace(method='ftp-plain',batches=lambda path,t:iter(table.to_batches()))
            result=suite.trial(client,'data.csv',10000,fixture,SimpleNamespace(remote=True))
            self.assertEqual(result['rows'],10);self.assertEqual(result['state'],'succeeded')
            self.assertIsNone(result['server_cpu_s']);self.assertIsNone(result['server_peak_rss_bytes'])
            self.assertGreater(result['client_peak_rss_bytes'],0)
    def test_publication_rejects_missing_trials(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'results.json'
            path.write_text(json.dumps(dict(config=dict(profile="final",repeats=3,warmups=1),samples=[],fixtures={'x':{}})))
            with self.assertRaises(ValueError):publish_filter.publish(path)

if __name__=='__main__':unittest.main()
