"""
This module provides comprehensive data quality validation capabilities including
schema validation, data completeness checks, accuracy validation, and anomaly detection.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
import pandas as pd
import numpy as np
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator
import structlog
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from ..utils.config_manager import ConfigManager
from ..utils.database_connector import DatabaseConnector

logger = structlog.get_logger()


class DataQualityValidator:
    """
    Comprehensive data quality validator for data engineering pipelines.
    
    This class provides advanced data quality validation capabilities including:
    - Schema validation and drift detection
    - Data completeness and null value analysis
    - Data accuracy and business rule validation
    - Statistical anomaly detection
    - Data profiling and metadata analysis
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize the data quality validator.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.db_connector = DatabaseConnector(config_manager)
        self.great_expectations_context = self._initialize_great_expectations()
        
        # Quality thresholds
        self.quality_thresholds = self.config_manager.get_config("quality_thresholds", {})
        
        logger.info("Data quality validator initialized")
    
    def _initialize_great_expectations(self) -> BaseDataContext:
        """
        Initialize Great Expectations context.
        
        Returns:
            Great Expectations data context
        """
        try:
            context = BaseDataContext(project_config_dir="./great_expectations")
            logger.info("Great Expectations context initialized")
            return context
        except Exception as e:
            logger.warning("Failed to initialize Great Expectations context", error=str(e))
            return None
    
    async def validate_schema(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data schema against expected schema.
        
        Args:
            config: Schema validation configuration
            
        Returns:
            Validation results
        """
        logger.info("Starting schema validation")
        
        try:
            # Get expected schema from config
            expected_schema = config.get("expected_schema", {})
            table_name = config.get("table_name")
            database_config = config.get("database", {})
            
            if not expected_schema or not table_name:
                return {
                    "valid": False,
                    "error": "Missing expected_schema or table_name in config",
                    "summary": "Schema validation failed due to missing configuration"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Get actual schema
            actual_schema = await self._get_table_schema(engine, table_name)
            
            # Compare schemas
            schema_comparison = self._compare_schemas(expected_schema, actual_schema)
            
            # Generate detailed report
            validation_result = {
                "valid": schema_comparison["is_valid"],
                "expected_schema": expected_schema,
                "actual_schema": actual_schema,
                "differences": schema_comparison["differences"],
                "missing_columns": schema_comparison["missing_columns"],
                "extra_columns": schema_comparison["extra_columns"],
                "type_mismatches": schema_comparison["type_mismatches"],
                "summary": f"Schema validation {'passed' if schema_comparison['is_valid'] else 'failed'}"
            }
            
            logger.info("Schema validation completed", 
                       valid=validation_result["valid"],
                       differences_count=len(schema_comparison["differences"]))
            
            return validation_result
            
        except Exception as e:
            logger.error("Schema validation failed", error=str(e))
            return {
                "valid": False,
                "error": str(e),
                "summary": f"Schema validation failed: {str(e)}"
            }
    
    async def validate_completeness(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data completeness and null value analysis.
        
        Args:
            config: Completeness validation configuration
            
        Returns:
            Validation results
        """
        logger.info("Starting data completeness validation")
        
        try:
            table_name = config.get("table_name")
            database_config = config.get("database", {})
            completeness_rules = config.get("completeness_rules", {})
            
            if not table_name:
                return {
                    "valid": False,
                    "error": "Missing table_name in config",
                    "summary": "Completeness validation failed due to missing configuration"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Get data for analysis
            df = await self._get_table_data(engine, table_name, limit=10000)
            
            # Analyze null values
            null_analysis = self._analyze_null_values(df)
            
            # Check completeness rules
            completeness_results = self._check_completeness_rules(df, completeness_rules)
            
            # Calculate overall completeness score
            total_cells = df.size
            null_cells = df.isnull().sum().sum()
            completeness_score = ((total_cells - null_cells) / total_cells) * 100
            
            validation_result = {
                "valid": completeness_score >= self.quality_thresholds.get("completeness_threshold", 95),
                "completeness_score": completeness_score,
                "null_analysis": null_analysis,
                "completeness_results": completeness_results,
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "total_cells": total_cells,
                "null_cells": null_cells,
                "summary": f"Data completeness: {completeness_score:.2f}%"
            }
            
            logger.info("Completeness validation completed", 
                       valid=validation_result["valid"],
                       completeness_score=completeness_score)
            
            return validation_result
            
        except Exception as e:
            logger.error("Completeness validation failed", error=str(e))
            return {
                "valid": False,
                "error": str(e),
                "summary": f"Completeness validation failed: {str(e)}"
            }
    
    async def validate_accuracy(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate data accuracy and business rules.
        
        Args:
            config: Accuracy validation configuration
            
        Returns:
            Validation results
        """
        logger.info("Starting data accuracy validation")
        
        try:
            table_name = config.get("table_name")
            database_config = config.get("database", {})
            accuracy_rules = config.get("accuracy_rules", {})
            
            if not table_name:
                return {
                    "valid": False,
                    "error": "Missing table_name in config",
                    "summary": "Accuracy validation failed due to missing configuration"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Get data for analysis
            df = await self._get_table_data(engine, table_name, limit=10000)
            
            # Apply accuracy rules
            accuracy_results = self._apply_accuracy_rules(df, accuracy_rules)
            
            # Calculate accuracy score
            total_checks = len(accuracy_results)
            passed_checks = len([r for r in accuracy_results if r["passed"]])
            accuracy_score = (passed_checks / total_checks) * 100 if total_checks > 0 else 100
            
            validation_result = {
                "valid": accuracy_score >= self.quality_thresholds.get("accuracy_threshold", 95),
                "accuracy_score": accuracy_score,
                "accuracy_results": accuracy_results,
                "total_checks": total_checks,
                "passed_checks": passed_checks,
                "failed_checks": total_checks - passed_checks,
                "summary": f"Data accuracy: {accuracy_score:.2f}%"
            }
            
            logger.info("Accuracy validation completed", 
                       valid=validation_result["valid"],
                       accuracy_score=accuracy_score)
            
            return validation_result
            
        except Exception as e:
            logger.error("Accuracy validation failed", error=str(e))
            return {
                "valid": False,
                "error": str(e),
                "summary": f"Accuracy validation failed: {str(e)}"
            }
    
    async def detect_anomalies(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect statistical anomalies in data.
        
        Args:
            config: Anomaly detection configuration
            
        Returns:
            Anomaly detection results
        """
        logger.info("Starting anomaly detection")
        
        try:
            table_name = config.get("table_name")
            database_config = config.get("database", {})
            columns_to_check = config.get("columns", [])
            
            if not table_name:
                return {
                    "valid": False,
                    "error": "Missing table_name in config",
                    "summary": "Anomaly detection failed due to missing configuration"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Get data for analysis
            df = await self._get_table_data(engine, table_name, limit=10000)
            
            # Detect anomalies in specified columns
            anomalies = {}
            for column in columns_to_check:
                if column in df.columns:
                    column_anomalies = self._detect_column_anomalies(df[column])
                    anomalies[column] = column_anomalies
            
            # Calculate overall anomaly score
            total_anomalies = sum(len(anomaly_list) for anomaly_list in anomalies.values())
            total_values = df[columns_to_check].size if columns_to_check else df.size
            anomaly_percentage = (total_anomalies / total_values) * 100 if total_values > 0 else 0
            
            detection_result = {
                "valid": anomaly_percentage <= self.quality_thresholds.get("anomaly_threshold", 5),
                "anomaly_percentage": anomaly_percentage,
                "total_anomalies": total_anomalies,
                "anomalies_by_column": anomalies,
                "summary": f"Anomaly detection: {anomaly_percentage:.2f}% anomalies found"
            }
            
            logger.info("Anomaly detection completed", 
                       valid=detection_result["valid"],
                       anomaly_percentage=anomaly_percentage)
            
            return detection_result
            
        except Exception as e:
            logger.error("Anomaly detection failed", error=str(e))
            return {
                "valid": False,
                "error": str(e),
                "summary": f"Anomaly detection failed: {str(e)}"
            }
    
    async def profile_data(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate comprehensive data profile.
        
        Args:
            config: Data profiling configuration
            
        Returns:
            Data profiling results
        """
        logger.info("Starting data profiling")
        
        try:
            table_name = config.get("table_name")
            database_config = config.get("database", {})
            
            if not table_name:
                return {
                    "valid": False,
                    "error": "Missing table_name in config",
                    "summary": "Data profiling failed due to missing configuration"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Get data for analysis
            df = await self._get_table_data(engine, table_name, limit=10000)
            
            # Generate comprehensive profile
            profile = self._generate_data_profile(df)
            
            profiling_result = {
                "valid": True,
                "table_name": table_name,
                "profile": profile,
                "summary": f"Data profile generated for {table_name}"
            }
            
            logger.info("Data profiling completed", table_name=table_name)
            
            return profiling_result
            
        except Exception as e:
            logger.error("Data profiling failed", error=str(e))
            return {
                "valid": False,
                "error": str(e),
                "summary": f"Data profiling failed: {str(e)}"
            }
    
    async def _get_table_schema(self, engine, table_name: str) -> Dict[str, Any]:
        """Get table schema from database."""
        try:
            with engine.connect() as connection:
                # Get column information
                query = text(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                
                result = connection.execute(query)
                columns = []
                
                for row in result:
                    columns.append({
                        "name": row.column_name,
                        "type": row.data_type,
                        "nullable": row.is_nullable == "YES",
                        "default": row.column_default
                    })
                
                return {"columns": columns}
                
        except SQLAlchemyError as e:
            logger.error("Failed to get table schema", error=str(e))
            raise
    
    async def _get_table_data(self, engine, table_name: str, limit: int = 1000) -> pd.DataFrame:
        """Get table data as pandas DataFrame."""
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            df = pd.read_sql(query, engine)
            return df
            
        except Exception as e:
            logger.error("Failed to get table data", error=str(e))
            raise
    
    def _compare_schemas(self, expected: Dict[str, Any], actual: Dict[str, Any]) -> Dict[str, Any]:
        """Compare expected and actual schemas."""
        expected_columns = {col["name"]: col for col in expected.get("columns", [])}
        actual_columns = {col["name"]: col for col in actual.get("columns", [])}
        
        differences = []
        missing_columns = []
        extra_columns = []
        type_mismatches = []
        
        # Check for missing columns
        for col_name in expected_columns:
            if col_name not in actual_columns:
                missing_columns.append(col_name)
                differences.append(f"Missing column: {col_name}")
        
        # Check for extra columns
        for col_name in actual_columns:
            if col_name not in expected_columns:
                extra_columns.append(col_name)
                differences.append(f"Extra column: {col_name}")
        
        # Check for type mismatches
        for col_name in expected_columns:
            if col_name in actual_columns:
                expected_type = expected_columns[col_name]["type"]
                actual_type = actual_columns[col_name]["type"]
                
                if expected_type != actual_type:
                    type_mismatches.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": actual_type
                    })
                    differences.append(f"Type mismatch in {col_name}: expected {expected_type}, got {actual_type}")
        
        return {
            "is_valid": len(differences) == 0,
            "differences": differences,
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "type_mismatches": type_mismatches
        }
    
    def _analyze_null_values(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze null values in DataFrame."""
        null_counts = df.isnull().sum()
        null_percentages = (null_counts / len(df)) * 100
        
        return {
            "null_counts": null_counts.to_dict(),
            "null_percentages": null_percentages.to_dict(),
            "columns_with_nulls": null_counts[null_counts > 0].index.tolist(),
            "total_null_cells": null_counts.sum()
        }
    
    def _check_completeness_rules(self, df: pd.DataFrame, rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check data completeness rules."""
        results = []
        
        for rule_name, rule_config in rules.items():
            column = rule_config.get("column")
            max_null_percentage = rule_config.get("max_null_percentage", 0)
            
            if column in df.columns:
                null_percentage = (df[column].isnull().sum() / len(df)) * 100
                passed = null_percentage <= max_null_percentage
                
                results.append({
                    "rule_name": rule_name,
                    "column": column,
                    "passed": passed,
                    "null_percentage": null_percentage,
                    "max_allowed": max_null_percentage
                })
        
        return results
    
    def _apply_accuracy_rules(self, df: pd.DataFrame, rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply accuracy validation rules."""
        results = []
        
        for rule_name, rule_config in rules.items():
            rule_type = rule_config.get("type")
            column = rule_config.get("column")
            
            if column not in df.columns:
                results.append({
                    "rule_name": rule_name,
                    "passed": False,
                    "error": f"Column {column} not found"
                })
                continue
            
            if rule_type == "range":
                min_val = rule_config.get("min")
                max_val = rule_config.get("max")
                
                if min_val is not None:
                    passed_min = (df[column] >= min_val).all()
                else:
                    passed_min = True
                
                if max_val is not None:
                    passed_max = (df[column] <= max_val).all()
                else:
                    passed_max = True
                
                passed = passed_min and passed_max
                
                results.append({
                    "rule_name": rule_name,
                    "passed": passed,
                    "column": column,
                    "rule_type": rule_type,
                    "min_value": min_val,
                    "max_value": max_val,
                    "violations": len(df[~((df[column] >= min_val) & (df[column] <= max_val))]) if min_val is not None and max_val is not None else 0
                })
            
            elif rule_type == "unique":
                is_unique = df[column].is_unique
                results.append({
                    "rule_name": rule_name,
                    "passed": is_unique,
                    "column": column,
                    "rule_type": rule_type,
                    "duplicate_count": len(df) - len(df[column].unique()) if not is_unique else 0
                })
            
            elif rule_type == "pattern":
                pattern = rule_config.get("pattern")
                if pattern:
                    matches_pattern = df[column].astype(str).str.match(pattern).all()
                    results.append({
                        "rule_name": rule_name,
                        "passed": matches_pattern,
                        "column": column,
                        "rule_type": rule_type,
                        "pattern": pattern,
                        "violations": len(df[~df[column].astype(str).str.match(pattern)])
                    })
        
        return results
    
    def _detect_column_anomalies(self, series: pd.Series) -> List[Dict[str, Any]]:
        """Detect anomalies in a single column."""
        anomalies = []
        
        # Remove null values for analysis
        clean_series = series.dropna()
        
        if len(clean_series) == 0:
            return anomalies
        
        # Statistical anomaly detection using IQR method
        Q1 = clean_series.quantile(0.25)
        Q3 = clean_series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Find outliers
        outliers = clean_series[(clean_series < lower_bound) | (clean_series > upper_bound)]
        
        for idx, value in outliers.items():
            anomalies.append({
                "index": idx,
                "value": value,
                "type": "outlier",
                "lower_bound": lower_bound,
                "upper_bound": upper_bound
            })
        
        return anomalies
    
    def _generate_data_profile(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive data profile."""
        profile = {
            "basic_info": {
                "shape": df.shape,
                "memory_usage": df.memory_usage(deep=True).sum(),
                "dtypes": df.dtypes.to_dict()
            },
            "statistics": {},
            "null_analysis": self._analyze_null_values(df),
            "unique_values": {},
            "value_counts": {}
        }
        
        # Generate statistics for numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        if len(numeric_columns) > 0:
            profile["statistics"]["numeric"] = df[numeric_columns].describe().to_dict()
        
        # Generate statistics for categorical columns
        categorical_columns = df.select_dtypes(include=['object']).columns
        if len(categorical_columns) > 0:
            profile["statistics"]["categorical"] = {}
            for col in categorical_columns:
                profile["statistics"]["categorical"][col] = {
                    "unique_count": df[col].nunique(),
                    "most_common": df[col].value_counts().head(5).to_dict()
                }
        
        # Unique value analysis
        for col in df.columns:
            profile["unique_values"][col] = df[col].nunique()
            if df[col].nunique() <= 20:  # Only show value counts for columns with few unique values
                profile["value_counts"][col] = df[col].value_counts().to_dict()
        
        return profile 