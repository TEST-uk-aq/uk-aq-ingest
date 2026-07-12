#!/usr/bin/env python3
"""
UK Air Quality History Integrity Validation
Phase 2c.1 Implementation
"""

import os
import sys
import json
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set, Tuple


class HistoryIntegrityValidator:
    """History Integrity Validator for UK Air Quality Data"""
    def __init__(self):
        # Domain-specific timestamp fields by data type
        self.domain_timestamp_fields = {
            'observations': ['observed_at', 'created_at', 'updated_at'],
            'aqi': ['calculated_at', 'valid_from', 'valid_until']
        }
        
        # Required parent aggregate fields
        self.required_parent_aggregate_fields = {
            'observations': ['connector_id', 'timeseries_id', 'observed_at'],
            'aqi': ['station_code', 'calculated_at', 'aqi_value']
        }
        
        # Required pollutant manifest fields
        self.required_pollutant_manifest_fields = [
            'pollutant_code', 'units', 'observation_count', 'min_value', 'max_value'
        ]
        
        # Data gap types
        self.data_gap_types = {
            'missing_parent_aggregate',
            'invalid_parent_aggregate', 
            'empty_timeseries_counts',
            'parquet_null_timeseries_id_rows',
            'timestamp_validation_failure',
            'pollutant_manifest_validation_failure'
        }
    
    def validate_parent_timestamp(self, data: Dict[str, Any], data_type: str) -> Tuple[bool, List[str]]:
        """
        Validate domain-specific parent timestamp fields
        """
        errors = []
        timestamp_fields = self.domain_timestamp_fields.get(data_type, [])
        
        for field in timestamp_fields:
            if field not in data:
                errors.append(f"Missing required timestamp field: {field}")
                continue
                
            timestamp_value = data[field]
            if not self._is_valid_timestamp(timestamp_value):
                errors.append(f"Invalid timestamp format for {field}: {timestamp_value}")
        
        return (len(errors) == 0, errors)
    
    def _is_valid_timestamp(self, value: Any) -> bool:
        """
        Check if value is a valid timestamp
        """
        if value is None:
            return False
            
        if isinstance(value, str):
            # Try to parse ISO format
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
                return True
            except (ValueError, AttributeError):
                return False
                
        if isinstance(value, (int, float)):
            # Unix timestamp
            try:
                datetime.fromtimestamp(value, tz=timezone.utc)
                return True
            except (ValueError, OSError):
                return False
                
        if isinstance(value, datetime):
            return True
            
        return False
    
    def validate_parent_aggregate(self, aggregate: Dict[str, Any], data_type: str) -> Tuple[bool, List[str]]:
        """
        Validate parent aggregate presence, type, and value
        """
        errors = []
        required_fields = self.required_parent_aggregate_fields.get(data_type, [])
        
        # Check presence
        for field in required_fields:
            if field not in aggregate:
                errors.append(f"Missing required aggregate field: {field}")
                continue
                
        # Check types and values
        if data_type == 'observations':
            if 'connector_id' in aggregate and not isinstance(aggregate['connector_id'], int):
                errors.append(f"Invalid type for connector_id: {type(aggregate['connector_id'])}")
                
            if 'timeseries_id' in aggregate and not isinstance(aggregate['timeseries_id'], int):
                errors.append(f"Invalid type for timeseries_id: {type(aggregate['timeseries_id'])}")
                
            if 'observed_at' in aggregate and not self._is_valid_timestamp(aggregate['observed_at']):
                errors.append(f"Invalid observed_at timestamp: {aggregate['observed_at']}")
                
        elif data_type == 'aqi':
            if 'station_code' in aggregate and not isinstance(aggregate['station_code'], str):
                errors.append(f"Invalid type for station_code: {type(aggregate['station_code'])}")
                
            if 'aqi_value' in aggregate and not isinstance(aggregate['aqi_value'], (int, float)):
                errors.append(f"Invalid type for aqi_value: {type(aggregate['aqi_value'])}")
                
            if 'calculated_at' in aggregate and not self._is_valid_timestamp(aggregate['calculated_at']):
                errors.append(f"Invalid calculated_at timestamp: {aggregate['calculated_at']}")
        
        return (len(errors) == 0, errors)
    
    def validate_pollutant_manifest(self, manifest: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate pollutant manifest fields
        """
        errors = []
        
        for field in self.required_pollutant_manifest_fields:
            if field not in manifest:
                errors.append(f"Missing required manifest field: {field}")
                continue
                
            # Type validation
            if field == 'pollutant_code' and not isinstance(manifest[field], str):
                errors.append(f"Invalid type for {field}: {type(manifest[field])}")
            elif field == 'units' and not isinstance(manifest[field], str):
                errors.append(f"Invalid type for {field}: {type(manifest[field])}")
            elif field in ['observation_count', 'min_value', 'max_value'] and not isinstance(manifest[field], (int, float)):
                errors.append(f"Invalid type for {field}: {type(manifest[field])}")
        
        return (len(errors) == 0, errors)
    
    def _read_parquet_partition_stats(self, partition_path: str) -> Dict[str, Any]:
        """
        Read parquet partition statistics including null timeseries_id counts
        """
        # This would normally use pyarrow or similar, but for Phase 2c.1 we'll mock the structure
        stats = {
            'row_count': 0,
            'null_timeseries_id_rows': 0,
            'non_null_timeseries_id_rows': 0,
            'min_timestamp': None,
            'max_timestamp': None
        }
        
        # In a real implementation, this would read the parquet file metadata
        # For now, we'll return a mock structure
        return stats
    
    def _append_parent_aggregate_gaps(self, gaps: List[Dict[str, Any]], 
                                     aggregate: Dict[str, Any], 
                                     data_type: str) -> None:
        """
        Append parent aggregate gaps to the gaps list
        """
        # Validate parent aggregate
        is_valid, errors = self.validate_parent_aggregate(aggregate, data_type)
        
        if not is_valid:
            for error in errors:
                gaps.append({
                    'gap_type': 'invalid_parent_aggregate',
                    'data_type': data_type,
                    'error': error,
                    'aggregate': aggregate
                })
        else:
            # Check for missing aggregates (would be detected elsewhere)
            pass
    
    def detect_data_gaps(self, data_manifest: Dict[str, Any], 
                        parquet_stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Detect various types of data gaps
        """
        gaps = []
        
        # Check for empty timeseries counts
        if 'timeseries_counts' in data_manifest:
            timeseries_counts = data_manifest['timeseries_counts']
            if isinstance(timeseries_counts, dict):
                for timeseries_id, count in timeseries_counts.items():
                    if count == 0:
                        gaps.append({
                            'gap_type': 'empty_timeseries_counts',
                            'timeseries_id': timeseries_id,
                            'count': count
                        })
            elif timeseries_counts == 0:
                gaps.append({
                    'gap_type': 'data_manifest_empty_timeseries_counts',
                    'total_count': 0
                })
        
        # Check for null timeseries_id rows in parquet
        if 'null_timeseries_id_rows' in parquet_stats:
            null_count = parquet_stats['null_timeseries_id_rows']
            if null_count > 0:
                gaps.append({
                    'gap_type': 'parquet_null_timeseries_id_rows',
                    'null_count': null_count,
                    'total_rows': parquet_stats.get('row_count', 0)
                })
        
        # Check for missing vs invalid vs genuine zero-row cases
        if 'row_count' in parquet_stats:
            row_count = parquet_stats['row_count']
            if row_count == 0:
                # Determine if this is genuine zero or missing data
                if 'expected_row_count' in data_manifest and data_manifest['expected_row_count'] > 0:
                    gaps.append({
                        'gap_type': 'missing_data',
                        'expected_count': data_manifest['expected_row_count'],
                        'actual_count': row_count
                    })
                else:
                    gaps.append({
                        'gap_type': 'genuine_zero_row_partition',
                        'row_count': row_count
                    })
        
        return gaps


def main():
    """
    Main entry point for history integrity validation
    """
    validator = HistoryIntegrityValidator()
    
    # Example usage (would be replaced with actual data loading)
    print("UK Air Quality History Integrity Validator - Phase 2c.1")
    print(f"Supported data gap types: {validator.data_gap_types}")
    
    # Test validation functions
    test_aggregate = {
        'connector_id': 123,
        'timeseries_id': 456,
        'observed_at': '2026-07-12T10:00:00Z'
    }
    
    is_valid, errors = validator.validate_parent_aggregate(test_aggregate, 'observations')
    print(f"Test aggregate validation: {'PASS' if is_valid else 'FAIL'}")
    if errors:
        print("Errors:", errors)
    
    # Test gap detection
    test_manifest = {
        'timeseries_counts': {1: 0, 2: 5, 3: 0},
        'expected_row_count': 10
    }
    
    test_parquet_stats = {
        'row_count': 5,
        'null_timeseries_id_rows': 2,
        'non_null_timeseries_id_rows': 3
    }
    
    gaps = validator.detect_data_gaps(test_manifest, test_parquet_stats)
    print(f"Detected {len(gaps)} data gaps:")
    for gap in gaps:
        print(f"  - {gap['gap_type']}: {gap}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())