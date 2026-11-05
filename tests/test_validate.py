"""Boundary checks for the public, read-only CSV validator (stdlib only)."""
import csv
import io
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from olist_pipeline import NUMERIC_COLUMNS, REQUIRED_COLUMNS, validate


class ValidateTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'snapshot.csv'
        self.row = ['order-1', '2017-07-30T23:59:59', '100', '12.5', '500', '1',
                    '2', '15', 'SP', 'RJ', 'café', 'credit_card', '0']

    def write(self, rows=None, header=REQUIRED_COLUMNS):
        with self.path.open('w', encoding='utf-8', newline='') as output:
            writer = csv.writer(output)
            writer.writerow(header)
            writer.writerows([self.row] if rows is None else rows)

    def test_valid_csv_and_no_side_effects(self):
        self.write()
        before = self.path.read_bytes()
        cwd = Path.cwd()
        original_open = io.open

        def readonly_open(file, mode='r', *args, **kwargs):
            if any(flag in mode for flag in 'wax+'):
                raise AssertionError('Validator attempted a write')
            return original_open(file, mode, *args, **kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch('io.open', side_effect=readonly_open))
            for name in ('os.mkdir', 'os.makedirs', 'os.chdir', 'os.remove', 'os.rename',
                         'os.replace', 'os.unlink', 'socket.socket', 'subprocess.Popen'):
                stack.enter_context(patch(name, side_effect=AssertionError(name)))
            self.assertEqual(validate(str(self.path)), str(self.path.resolve()))
        self.assertEqual(before, self.path.read_bytes())
        self.assertEqual(cwd, Path.cwd())
        self.assertEqual([self.path], list(self.path.parent.iterdir()))

    def test_each_missing_column(self):
        for index, name in enumerate(REQUIRED_COLUMNS):
            with self.subTest(column=name):
                self.write([self.row[:index] + self.row[index+1:]],
                           REQUIRED_COLUMNS[:index] + REQUIRED_COLUMNS[index+1:])
                with self.assertRaises(ValueError):
                    validate(self.path)

    def test_nonfinite_and_invalid_numeric_values(self):
        for column in (*NUMERIC_COLUMNS, 'late'):
            for value in ('NaN', 'inf', '-inf', '1e999', '', 'abc'):
                with self.subTest(column=column, value=value):
                    row = self.row.copy()
                    row[REQUIRED_COLUMNS.index(column)] = value
                    self.write([row])
                    with self.assertRaises(ValueError):
                        validate(self.path)

    def test_nonbinary_labels(self):
        for value in ('-1', '2', '0.5'):
            with self.subTest(value=value):
                self.write([self.row[:-1] + [value]])
                with self.assertRaises(ValueError):
                    validate(self.path)

    def test_duplicates_and_missing_id(self):
        self.write([self.row, self.row])
        with self.assertRaisesRegex(ValueError, 'duplicate order_id'):
            validate(self.path)
        self.write([[' '] + self.row[1:]])
        with self.assertRaises(ValueError):
            validate(self.path)

    def test_empty_and_header_only(self):
        self.path.write_text('')
        with self.assertRaises(ValueError):
            validate(self.path)
        self.write([])
        with self.assertRaises(ValueError):
            validate(self.path)

    def test_invalid_dates_and_ragged_records(self):
        for value in ('', 'NaT', '2017-02-30', 'not-a-date'):
            self.write([[self.row[0], value] + self.row[2:]])
            with self.assertRaises(ValueError):
                validate(self.path)
        for row in (self.row[:-1], self.row + ['extra']):
            self.write([row])
            with self.assertRaises(ValueError):
                validate(self.path)

    def test_duplicate_header(self):
        self.write([self.row + ['1']], (*REQUIRED_COLUMNS, 'price'))
        with self.assertRaises(ValueError):
            validate(self.path)

    def test_invalid_utf8_and_compressed_input(self):
        self.path.write_bytes(b'\xff')
        with self.assertRaises(UnicodeError):
            validate(self.path)
        with self.assertRaises(ValueError):
            validate(str(self.path) + '.gz')


if __name__ == '__main__':
    unittest.main()
