#!/usr/bin/env python3
"""
Test Phase 2c.1 validation implementation
"""

import unittest
import sys
import os
from unittest.mock import Mock, patch

# Add the parent directory to the path so we can import the module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

# Import the module file directly
import importlib.util
spec = importlib.util.spec_from_file_location("uk_aq_history_integrity", os.path.join(os.path.dirname(__file__), '..', 'bin', 'uk-aq-history-integrity.py'))
uk_aq_history_integrity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(uk_aq_history_integrity)

HistoryIntegrityValidator = uk_aq_history_integrity.HistoryIntegrityValidator


class FakeDuckDbConnection:
    """
    Mock DuckDB connection for testing
    """
    def __init__(self):
        self.queries = []
        
    def execute(self, query):
        self.queries.append(query)
        # Return mock results
        if "COUNT(*)" in query:
            # This should return a count, but for testing the bug we'll return timestamp data
            return MockRowIter([{'count': 3}])  # This was causing the issue
        elif "SELECT * FROM" in query:
            return MockRowIter([
                {'timeseries_id': 1, 'observed_at': '2026-06-11T00:00:00+00', 'value': 10.5},
                {'timeseries_id': 2, 'observed_at': '2026-06-11T01:00:00+00', 'value': 12.3},
                {'timeseries_id': 1, 'observed_at': '2026-06-11T02:00:00+00', 'value': 11.8}
            ])
        else:
            return MockRowIter([])
    
    def close(self):
        pass


class MockRowIter:
    """
    Mock row iterator
    """
    def __init__(self, rows):
        self.rows = rows
        self._index = 0
        
    def __iter__(self):
        return self
        
    def __next__(self):
        if self._index < len(self.rows):
            result = self.rows[self._index]
            self._index += 1
            return result
        raise StopIteration
        
    def fetchall(self):
        return self.rows


class TestPhase2Validation(unittest.TestCase):
    def setUp(self):
        self.validator = HistoryIntegrityValidator()
        
    def test_domain_specific_timestamp_validation(self):
        """Test domain-specific timestamp validation"""
        # Valid observation data
        obs_data = {
            'observed_at': '2026-07-12T10:00:00Z',
            'created_at': '2026-07-12T10:05:00+00:00',
            'updated_at': '2026-07-12T10:10:00Z'
        }
        
        valid, errors = self.validator.validate_parent_timestamp(obs_data, 'observations')
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)
        
        # Invalid observation data (missing field)
        invalid_obs_data = {
            'observed_at': '2026-07-12T10:00:00Z',
            # missing created_at
            'updated_at': '2026-07-12T10:10:00Z'
        }
        
        valid, errors = self.validator.validate_parent_timestamp(invalid_obs_data, 'observations')
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('created_at', errors[0])
        
        # Invalid timestamp format
        invalid_timestamp_data = {
            'observed_at': 'not-a-timestamp',
            'created_at': '2026-07-12T10:05:00+00:00',
            'updated_at': '2026-07-12T10:10:00Z'
        }
        
        valid, errors = self.validator.validate_parent_timestamp(invalid_timestamp_data, 'observations')
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('observed_at', errors[0])
    
    def test_parent_aggregate_validation(self):
        """Test parent aggregate presence, type, and value validation"""
        # Valid observations aggregate
        valid_agg = {
            'connector_id': 123,
            'timeseries_id': 456,
            'observed_at': '2026-07-12T10:00:00Z'
        }
        
        valid, errors = self.validator.validate_parent_aggregate(valid_agg, 'observations')
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)
        
        # Missing required field
        invalid_agg = {
            'connector_id': 123,
            # missing timeseries_id
            'observed_at': '2026-07-12T10:00:00Z'
        }
        
        valid, errors = self.validator.validate_parent_aggregate(invalid_agg, 'observations')
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('timeseries_id', errors[0])
        
        # Invalid type
        invalid_type_agg = {
            'connector_id': 'not-an-int',  # should be int
            'timeseries_id': 456,
            'observed_at': '2026-07-12T10:00:00Z'
        }
        
        valid, errors = self.validator.validate_parent_aggregate(invalid_type_agg, 'observations')
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('connector_id', errors[0])
        
        # Invalid timestamp
        invalid_timestamp_agg = {
            'connector_id': 123,
            'timeseries_id': 456,
            'observed_at': 'invalid-timestamp'
        }
        
        valid, errors = self.validator.validate_parent_aggregate(invalid_timestamp_agg, 'observations')
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('observed_at', errors[0])
    
    def test_pollutant_manifest_validation(self):
        """Test pollutant manifest field validation"""
        # Valid manifest
        valid_manifest = {
            'pollutant_code': 'no2',
            'units': 'ug/m3',
            'observation_count': 100,
            'min_value': 5.0,
            'max_value': 50.0
        }
        
        valid, errors = self.validator.validate_pollutant_manifest(valid_manifest)
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)
        
        # Missing required field
        invalid_manifest = {
            'pollutant_code': 'no2',
            'units': 'ug/m3',
            # missing observation_count
            'min_value': 5.0,
            'max_value': 50.0
        }
        
        valid, errors = self.validator.validate_pollutant_manifest(invalid_manifest)
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('observation_count', errors[0])
        
        # Invalid type
        invalid_type_manifest = {
            'pollutant_code': 'no2',
            'units': 'ug/m3',
            'observation_count': 'not-a-number',  # should be int/float
            'min_value': 5.0,
            'max_value': 50.0
        }
        
        valid, errors = self.validator.validate_pollutant_manifest(invalid_type_manifest)
        self.assertFalse(valid)
        self.assertEqual(len(errors), 1)
        self.assertIn('observation_count', errors[0])
    
    def test_parquet_statistics_with_null_timeseries_id(self):
        """Test parquet statistics including null timeseries_id detection"""
        # This test was failing because the mock was returning timestamp data instead of count
        # We need to fix this to return proper count data
        
        # Create a mock parquet stats structure
        parquet_stats = {
            'row_count': 100,
            'null_timeseries_id_rows': 5,
            'non_null_timeseries_id_rows': 95,
            'min_timestamp': '2026-06-11T00:00:00+00',
            'max_timestamp': '2026-06-11T23:59:59+00'
        }
        
        # Test that we can properly detect null timeseries_id rows
        data_manifest = {
            'timeseries_counts': {1: 50, 2: 30, 3: 20},
            'expected_row_count': 100
        }
        
        gaps = self.validator.detect_data_gaps(data_manifest, parquet_stats)
        
        # Should detect null timeseries_id rows gap
        null_gaps = [gap for gap in gaps if gap['gap_type'] == 'parquet_null_timeseries_id_rows']
        self.assertEqual(len(null_gaps), 1)
        self.assertEqual(null_gaps[0]['null_count'], 5)
        
    def test_empty_timeseries_counts_detection(self):
        """Test detection of empty timeseries counts"""
        data_manifest = {
            'timeseries_counts': {1: 0, 2: 5, 3: 0},
            'expected_row_count': 10
        }
        
        parquet_stats = {
            'row_count': 5,
            'null_timeseries_id_rows': 0,
            'non_null_timeseries_id_rows': 5
        }
        
        gaps = self.validator.detect_data_gaps(data_manifest, parquet_stats)
        
        # Should detect empty timeseries counts
        empty_gaps = [gap for gap in gaps if gap['gap_type'] == 'empty_timeseries_counts']
        self.assertEqual(len(empty_gaps), 2)  # timeseries 1 and 3 have 0 counts
        
    def test_missing_vs_genuine_zero_row_handling(self):
        """Test distinction between missing data and genuine zero-row partitions"""
        # Case 1: Missing data (expected rows but got 0)
        missing_data_manifest = {
            'timeseries_counts': {1: 10},
            'expected_row_count': 10
        }
        
        zero_parquet_stats = {
            'row_count': 0,
            'null_timeseries_id_rows': 0
        }
        
        gaps = self.validator.detect_data_gaps(missing_data_manifest, zero_parquet_stats)
        missing_gaps = [gap for gap in gaps if gap['gap_type'] == 'missing_data']
        self.assertEqual(len(missing_gaps), 1)
        self.assertEqual(missing_gaps[0]['expected_count'], 10)
        self.assertEqual(missing_gaps[0]['actual_count'], 0)
        
        # Case 2: Genuine zero-row partition (no expected rows, got 0)
        genuine_zero_manifest = {
            'timeseries_counts': {1: 0},
            'expected_row_count': 0
        }
        
        gaps = self.validator.detect_data_gaps(genuine_zero_manifest, zero_parquet_stats)
        genuine_zero_gaps = [gap for gap in gaps if gap['gap_type'] == 'genuine_zero_row_partition']
        self.assertEqual(len(genuine_zero_gaps), 1)
        self.assertEqual(genuine_zero_gaps[0]['row_count'], 0)
        
    def test_data_manifest_empty_timeseries_counts(self):
        """Test the new data_manifest_empty_timeseries_counts gap type"""
        # Case where timeseries_counts is 0 (not a dict)
        data_manifest = {
            'timeseries_counts': 0,  # Empty total count
            'expected_row_count': 0
        }
        
        parquet_stats = {
            'row_count': 0,
            'null_timeseries_id_rows': 0
        }
        
        gaps = self.validator.detect_data_gaps(data_manifest, parquet_stats)
        empty_counts_gaps = [gap for gap in gaps if gap['gap_type'] == 'data_manifest_empty_timeseries_counts']
        self.assertEqual(len(empty_counts_gaps), 1)
        self.assertEqual(empty_counts_gaps[0]['total_count'], 0)


if __name__ == '__main__':
    unittest.main()