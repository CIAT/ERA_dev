#!/usr/bin/env python3
"""Stage date-only JSON from a pinned legacy snapshot; never publish or overwrite.

Date CSVs must come from the verified full merge replay in identical row order.
Decimal parsing/writing preserves all non-date numbers exactly, avoiding a fresh
jsonlite export's rounding changes. Output retains source table/row/key order.
"""
import argparse
import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

FIELDS = {
    'Data.Out': ['PD.Plant.Start', 'PD.Plant.End', 'PD.Harvest.Start',
                 'PD.Harvest.End', 'ED.Sample.Start', 'ED.Sample.End'],
    'Fert.Method': ['F.Date'],
    'Res.Method': ['M.Date'],
}


def encode(value):
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError('Non-finite JSON number')
        return str(value)
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k) + ':' + encode(v) for k, v in value.items()) + '}'
    if isinstance(value, list):
        return '[' + ','.join(map(encode, value)) + ']'
    return json.dumps(value, ensure_ascii=False, allow_nan=False)


def stage(source, patches, output, expected_sha256):
    if output.exists():
        raise ValueError('Output already exists')
    raw = source.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError('Source SHA256 mismatch')
    data = json.loads(raw, parse_float=Decimal)
    del raw
    changes = {}
    for table, fields in FIELDS.items():
        with (patches / (table + '-date-patch.csv')).open(newline='') as f:
            patch = list(csv.DictReader(f))
        rows = data[table]
        if len(rows) != len(patch):
            raise ValueError('Row count mismatch: ' + table)
        changes[table] = {field: 0 for field in fields}
        for i, (row, dates) in enumerate(zip(rows, patch), 1):
            if row.get('B.Code') != dates['B.Code'] or int(dates['merged_row']) != i:
                raise ValueError(f'Row identity mismatch: {table} row {i}')
            for field in fields:
                value = dates[field] or None
                if value is not None:
                    from datetime import date
                    if date.fromisoformat(value).isoformat() != value:
                        raise ValueError('Noncanonical date: ' + value)
                if row.get(field) != value:
                    changes[table][field] += 1
                if value is None:
                    row.pop(field, None)  # Preserve sparse JSON missing-date convention.
                else:
                    row[field] = value
    # Write each row separately; retain exact decimal values without one huge
    # serialized string. Exclusive creation prevents accidental replacement.
    with output.open('x', encoding='utf-8') as f:
        f.write('{\n')
        for ti, (table, rows) in enumerate(data.items()):
            if ti:
                f.write(',\n')
            f.write(json.dumps(table) + ':[\n')
            for ri, row in enumerate(rows):
                if ri:
                    f.write(',\n')
                f.write(encode(row))
            f.write('\n]')
        f.write('\n}\n')
    return changes


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('patches', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--source-sha256', required=True)
    args = parser.parse_args()
    print(json.dumps(stage(args.source, args.patches, args.output, args.source_sha256), indent=2))
