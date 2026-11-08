import hashlib
import tempfile
import unittest
from pathlib import Path
import suite

class TransferContracts(unittest.TestCase):
    def test_generated_fixtures_match_hashes_and_are_reproducible(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw);one=suite.generate(root/'one','smoke');two=suite.generate(root/'two','smoke')
            self.assertEqual(one,two)
            for name,entry in one.items():
                value=(root/'one'/name).read_bytes()
                self.assertEqual(len(value),entry['bytes']);self.assertEqual(hashlib.sha256(value).hexdigest(),entry['sha256'])
    def test_parallel_cases_measure_total_payload_and_fresh_is_per_trial(self):
        files={'large-100MiB.bin':{},'large-1GiB.bin':{},'small/0000.bin':{}}
        cases=suite.cases(files);self.assertEqual(len(cases),7)
        parallel=next(c for c in cases if c['name']=='parallel-8')
        self.assertEqual(len(parallel['paths']),8);self.assertEqual(parallel['parallelism'],8)
        fresh=next(c for c in cases if c['name']=='small-batch-fresh')
        self.assertEqual(fresh['connection'],'fresh');self.assertEqual(fresh['parallelism'],1)

if __name__=='__main__':unittest.main()
