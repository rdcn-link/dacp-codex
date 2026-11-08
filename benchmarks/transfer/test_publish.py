import copy
import json
import tempfile
import unittest
from pathlib import Path
import publish


class PublicationContracts(unittest.TestCase):
    def reject(self, data, message):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'results.json'
            original = json.dumps(data)
            source.write_text(original)
            with self.assertRaisesRegex(ValueError, message):
                publish.publish(source, root / 'docs')
            self.assertEqual(source.read_text(), original)
            self.assertFalse((root / 'docs').exists())

    def test_smoke_and_incomplete_batches_cannot_be_published_as_final(self):
        for profile in ['smoke', 'final']:
            self.reject({'config': {'profile': profile, 'warmups': 1, 'repeats': 3},
                         'cases': [{'name': 'large-100MiB'}], 'samples': []}, 'complete final-profile')

    def test_duplicate_trials_cannot_hide_missing_coverage(self):
        sample = {'case': 'large-100MiB', 'sink': 'consume',
                  'method': 'dacp-tls', 'repeat': 0}
        self.reject({'config': {'profile': 'final', 'warmups': 0, 'repeats': 1},
                     'cases': [{'name': 'large-100MiB'}], 'samples': [sample] * 6},
                    'Missing or duplicate')


    def test_complete_batch_preserves_failures_and_writes_bilingual_artifacts(self):
        source_path = Path(__file__).resolve().parents[2] / '.benchmark-runs/transfer-final/results.json'
        if not source_path.exists():
            self.skipTest('Optional local batch integration check')
        data = json.loads(source_path.read_text())
        cfg = data['config']
        expected = len(data['cases']) * 6 * (cfg['warmups'] + cfg['repeats'])
        if len(data['samples']) != expected:
            self.skipTest('Local batch is still running')
        data['runtime_inventory'] = json.loads((source_path.parent / 'runtime-inventory.json').read_text())
        sample = next(s for s in data['samples'] if not s['warmup'])
        sample['state'] = 'failed'
        sample['error'] = 'synthetic publication contract failure'
        original = copy.deepcopy(data)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'results.json'
            source.write_text(json.dumps(data))
            self.assertEqual(publish.publish(source, root / 'docs'), 1)
            self.assertEqual(json.loads(source.read_text()), original)
            for suffix in ['', '_zh']:
                self.assertTrue((root / 'docs' / ('transfer-benchmark' + suffix + '.md')).exists())
                for chart in ['throughput', 'wall', 'first-byte', 'client-cpu', 'server-cpu', 'server-rss', 'client-rss']:
                    self.assertTrue((root / 'docs/benchmarks/transfer' / (chart + suffix + '.svg')).exists())
            raw = next((root / 'docs/benchmarks/transfer').glob('*.json'))
            recorded = json.loads(raw.read_text())
            self.assertIn('synthetic publication contract failure', raw.read_text())
            self.assertEqual(len(recorded['samples']), expected)


if __name__ == '__main__':
    unittest.main()
