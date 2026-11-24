import csv
import hashlib
import importlib.util
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

spec = importlib.util.spec_from_file_location('stage', 'scripts/stage_issue94_json.py')
stage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stage)


class StagingTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source.json'
        self.output = self.root / 'output.json'
        raw = '{' + ','.join(json.dumps(t) + ':[{"B.Code":"X","value":1.234567890123456789}]' for t in stage.FIELDS) + '}'
        self.source.write_text(raw)
        self.sha = hashlib.sha256(self.source.read_bytes()).hexdigest()
        for table, fields in stage.FIELDS.items():
            with (self.root / (table + '-date-patch.csv')).open('w') as f:
                writer = csv.DictWriter(f, fieldnames=['B.Code', 'merged_row'] + fields)
                writer.writeheader()
                writer.writerow({'B.Code': 'X', 'merged_row': 1, **{k: '2005-07-17' for k in fields}})

    def test_dates_change_without_decimal_loss(self):
        stage.stage(self.source, self.root, self.output, self.sha)
        data = json.loads(self.output.read_text(), parse_float=Decimal)
        for table, fields in stage.FIELDS.items():
            self.assertEqual(data[table][0]['value'], Decimal('1.234567890123456789'))
            self.assertTrue(all(data[table][0][f] == '2005-07-17' for f in fields))
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), self.sha)

    def test_wrong_source_pin_stops(self):
        with self.assertRaisesRegex(ValueError, 'SHA256'):
            stage.stage(self.source, self.root, self.output, 'wrong')
        self.assertFalse(self.output.exists())

    def test_wrong_row_identity_stops(self):
        p = self.root / 'Data.Out-date-patch.csv'
        p.write_text(p.read_text().replace('X,1,', 'X,2,'))
        with self.assertRaisesRegex(ValueError, 'identity'):
            stage.stage(self.source, self.root, self.output, self.sha)
        self.assertFalse(self.output.exists())

    def test_existing_output_never_overwritten(self):
        self.output.write_text('keep')
        with self.assertRaisesRegex(ValueError, 'already exists'):
            stage.stage(self.source, self.root, self.output, self.sha)
        self.assertEqual(self.output.read_text(), 'keep')


if __name__ == '__main__':
    unittest.main()
