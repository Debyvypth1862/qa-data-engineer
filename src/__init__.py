"""
This package provides comprehensive testing capabilities for data engineering pipelines,
including data quality validation, performance testing, API testing, and monitoring.
"""

__version__ = "1.0.0"
__author__ = "Vy Pham"
__email__ = "vypmon@gmail.com"

from .core.test_framework import TestFramework
from .data_quality.validator import DataQualityValidator
from .performance.performance_tester import PerformanceTester
from .api.api_tester import APITester
from .monitoring.monitor import DataQualityMonitor

__all__ = [
    "TestFramework",
    "DataQualityValidator", 
    "PerformanceTester",
    "APITester",
    "DataQualityMonitor"
] 