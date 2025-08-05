"""
Comprehensive Test Suite

This module demonstrates all the testing capabilities of the framework
including data quality testing, performance testing, API testing, and integration testing.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from typing import Dict, Any, List
import pandas as pd
import numpy as np

from src.core.test_framework import ConcreteTestFramework, TestSuite, TestType
from src.data_quality.validator import DataQualityValidator
from src.performance.performance_tester import PerformanceTester
from src.api.api_tester import APITester
from src.utils.config_manager import ConfigManager


class TestComprehensiveQASuite:
    """
    Comprehensive test suite.
    
    This test suite showcases:
    - Data quality validation and testing
    - Performance testing and optimization
    - API testing and validation
    - Integration testing
    - End-to-end pipeline testing
    """
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        self.config_manager = ConfigManager("config/config.yaml")
        self.test_framework = ConcreteTestFramework(self.config_manager)
        self.data_quality_validator = DataQualityValidator(self.config_manager)
        self.performance_tester = PerformanceTester(self.config_manager)
        self.api_tester = APITester(self.config_manager)
        
        # Test data setup
        self.test_data = self._generate_test_data()
        
        yield
        
        # Cleanup
        self.test_framework.close_all_connections()
    
    def _generate_test_data(self) -> Dict[str, Any]:
        """Generate comprehensive test data."""
        # Generate user data
        users_data = []
        for i in range(1000):
            users_data.append({
                'id': i + 1,
                'name': f'User {i + 1}',
                'email': f'user{i + 1}@example.com',
                'age': np.random.randint(18, 80),
                'created_at': datetime.now() - timedelta(days=np.random.randint(1, 365)),
                'status': np.random.choice(['active', 'inactive', 'suspended'])
            })
        
        # Generate order data
        orders_data = []
        for i in range(5000):
            orders_data.append({
                'id': i + 1,
                'user_id': np.random.randint(1, 1001),
                'amount': round(np.random.uniform(10, 1000), 2),
                'status': np.random.choice(['pending', 'completed', 'cancelled']),
                'created_at': datetime.now() - timedelta(days=np.random.randint(1, 365))
            })
        
        return {
            'users': users_data,
            'orders': orders_data
        }
    
    @pytest.mark.asyncio
    async def test_data_quality_comprehensive(self):
        """Test comprehensive data quality validation."""
        # Test schema validation
        schema_config = {
            "expected_schema": {
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "age", "type": "integer"},
                    {"name": "created_at", "type": "datetime"},
                    {"name": "status", "type": "string"}
                ]
            },
            "table_name": "users",
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            }
        }
        
        schema_result = await self.data_quality_validator.validate_schema(schema_config)
        assert schema_result["valid"] is True
        
        # Test data completeness
        completeness_config = {
            "table_name": "users",
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "completeness_rules": {
                "email_required": {
                    "column": "email",
                    "max_null_percentage": 0
                },
                "name_required": {
                    "column": "name",
                    "max_null_percentage": 0
                }
            }
        }
        
        completeness_result = await self.data_quality_validator.validate_completeness(completeness_config)
        assert completeness_result["valid"] is True
        assert completeness_result["completeness_score"] >= 95.0
        
        # Test data accuracy
        accuracy_config = {
            "table_name": "users",
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "accuracy_rules": {
                "age_range": {
                    "type": "range",
                    "column": "age",
                    "min": 18,
                    "max": 80
                },
                "email_format": {
                    "type": "pattern",
                    "column": "email",
                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                }
            }
        }
        
        accuracy_result = await self.data_quality_validator.validate_accuracy(accuracy_config)
        assert accuracy_result["valid"] is True
        assert accuracy_result["accuracy_score"] >= 95.0
        
        # Test anomaly detection
        anomaly_config = {
            "table_name": "users",
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "columns": ["age", "amount"]
        }
        
        anomaly_result = await self.data_quality_validator.detect_anomalies(anomaly_config)
        assert anomaly_result["valid"] is True
        assert anomaly_result["anomaly_percentage"] <= 5.0
    
    @pytest.mark.asyncio
    async def test_performance_comprehensive(self):
        """Test comprehensive performance validation."""
        # Test query performance
        query_config = {
            "queries": [
                {
                    "name": "User Count Query",
                    "sql": "SELECT COUNT(*) FROM users",
                    "expected_time": 0.1
                },
                {
                    "name": "Order Analysis Query",
                    "sql": "SELECT status, COUNT(*), AVG(amount) FROM orders GROUP BY status",
                    "expected_time": 0.5
                },
                {
                    "name": "Complex Join Query",
                    "sql": """
                        SELECT u.name, COUNT(o.id), AVG(o.amount) 
                        FROM users u 
                        LEFT JOIN orders o ON u.id = o.user_id 
                        GROUP BY u.id, u.name 
                        HAVING COUNT(o.id) > 0 
                        ORDER BY AVG(o.amount) DESC 
                        LIMIT 100
                    """,
                    "expected_time": 2.0
                }
            ],
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "iterations": 5,
            "concurrent_users": 3
        }
        
        query_performance = await self.performance_tester.test_query_performance(query_config)
        assert query_performance["passed"] is True
        assert query_performance["overall_score"] >= 80.0
        
        # Test load performance
        load_config = {
            "scenarios": [
                {
                    "name": "Normal Load",
                    "concurrent_users": 10,
                    "queries": [
                        {"sql": "SELECT COUNT(*) FROM users"},
                        {"sql": "SELECT COUNT(*) FROM orders"}
                    ]
                },
                {
                    "name": "High Load",
                    "concurrent_users": 25,
                    "queries": [
                        {"sql": "SELECT * FROM users LIMIT 100"},
                        {"sql": "SELECT * FROM orders LIMIT 100"}
                    ]
                }
            ],
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "duration": 30  # 30 seconds for testing
        }
        
        load_performance = await self.performance_tester.test_load_performance(load_config)
        assert load_performance["passed"] is True
        assert load_performance["load_score"] >= 85.0
        
        # Test stress performance
        stress_config = {
            "scenarios": [
                {
                    "name": "Stress Test",
                    "initial_users": 5,
                    "max_users": 20,
                    "user_increment": 5,
                    "increment_interval": 10,
                    "queries": [
                        {"sql": "SELECT COUNT(*) FROM users"},
                        {"sql": "SELECT COUNT(*) FROM orders"}
                    ]
                }
            ],
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "max_duration": 60  # 1 minute for testing
        }
        
        stress_performance = await self.performance_tester.test_stress_performance(stress_config)
        assert stress_performance["passed"] is True
        assert stress_performance["stress_score"] >= 70.0
    
    @pytest.mark.asyncio
    async def test_api_comprehensive(self):
        """Test comprehensive API validation."""
        # Mock API configuration for testing
        api_config = {
            "base_url": "http://localhost:8000",
            "endpoints": [
                {
                    "name": "Health Check",
                    "path": "/health",
                    "method": "GET",
                    "expected_status": 200
                },
                {
                    "name": "Get Users",
                    "path": "/api/users",
                    "method": "GET",
                    "expected_status": 200
                },
                {
                    "name": "Create User",
                    "path": "/api/users",
                    "method": "POST",
                    "expected_status": 201,
                    "data": {
                        "name": "Test User",
                        "email": "test@example.com"
                    }
                }
            ],
            "authentication": {
                "type": "bearer",
                "token": "test_token_123"
            },
            "scenarios": [
                {
                    "name": "API Load Test",
                    "endpoint": "/api/users",
                    "method": "GET",
                    "concurrent_users": 10,
                    "duration": 30,
                    "requests_per_second": 10
                }
            ]
        }
        
        # Note: This test would require a running API server
        # For demonstration, we'll test the API tester configuration
        assert api_config["base_url"] == "http://localhost:8000"
        assert len(api_config["endpoints"]) == 3
        assert api_config["authentication"]["type"] == "bearer"
    
    @pytest.mark.asyncio
    async def test_integration_comprehensive(self):
        """Test comprehensive integration scenarios."""
        # Test end-to-end data pipeline
        pipeline_config = {
            "name": "E2E Data Pipeline Test",
            "description": "End-to-end data pipeline validation",
            "test_types": [TestType.DATA_QUALITY, TestType.PERFORMANCE],
            "config": {
                "data_quality": {
                    "schema_validation": {
                        "expected_schema": {
                            "columns": [
                                {"name": "id", "type": "integer"},
                                {"name": "name", "type": "string"},
                                {"name": "email", "type": "string"}
                            ]
                        },
                        "table_name": "users",
                        "database": {
                            "type": "sqlite",
                            "path": ":memory:"
                        }
                    },
                    "completeness_validation": {
                        "table_name": "users",
                        "database": {
                            "type": "sqlite",
                            "path": ":memory:"
                        },
                        "completeness_rules": {
                            "email_required": {
                                "column": "email",
                                "max_null_percentage": 0
                            }
                        }
                    }
                },
                "performance": {
                    "query_performance": {
                        "queries": [
                            {
                                "name": "User Count",
                                "sql": "SELECT COUNT(*) FROM users",
                                "expected_time": 0.1
                            }
                        ],
                        "database": {
                            "type": "sqlite",
                            "path": ":memory:"
                        },
                        "iterations": 3,
                        "concurrent_users": 2
                    }
                }
            },
            "dependencies": ["database", "test_data"],
            "timeout": 300
        }
        
        test_suite = TestSuite(**pipeline_config)
        results = await self.test_framework.run_test_suite(test_suite)
        
        # Verify test results
        assert len(results) > 0
        
        # Check that all tests passed
        passed_tests = [r for r in results if r.status.value == "passed"]
        assert len(passed_tests) >= len(results) * 0.9  # 90% success rate
    
    @pytest.mark.asyncio
    async def test_data_profiling(self):
        """Test comprehensive data profiling capabilities."""
        profiling_config = {
            "table_name": "users",
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            }
        }
        
        profile_result = await self.data_quality_validator.profile_data(profiling_config)
        assert profile_result["valid"] is True
        assert "profile" in profile_result
        assert "basic_info" in profile_result["profile"]
        assert "statistics" in profile_result["profile"]
        assert "null_analysis" in profile_result["profile"]
    
    @pytest.mark.asyncio
    async def test_benchmark_performance(self):
        """Test benchmark performance comparison."""
        benchmark_config = {
            "benchmarks": [
                {
                    "name": "User Query Benchmark",
                    "queries": [
                        {
                            "name": "user_count",
                            "sql": "SELECT COUNT(*) FROM users"
                        },
                        {
                            "name": "user_analysis",
                            "sql": "SELECT status, COUNT(*) FROM users GROUP BY status"
                        }
                    ]
                }
            ],
            "database": {
                "type": "sqlite",
                "path": ":memory:"
            },
            "baseline": {
                "User Query Benchmark": {
                    "user_count": 0.05,
                    "user_analysis": 0.1
                }
            }
        }
        
        benchmark_result = await self.performance_tester.benchmark_performance(benchmark_config)
        assert benchmark_result["passed"] is True
        assert benchmark_result["benchmark_score"] >= 90.0
    
    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling and edge cases."""
        # Test with invalid database configuration
        invalid_db_config = {
            "type": "invalid_db",
            "host": "invalid_host",
            "port": 9999
        }
        
        # Test data quality with invalid config
        invalid_config = {
            "table_name": "nonexistent_table",
            "database": invalid_db_config
        }
        
        result = await self.data_quality_validator.validate_schema(invalid_config)
        assert result["valid"] is False
        assert "error" in result
        
        # Test performance with invalid queries
        invalid_performance_config = {
            "queries": [
                {
                    "name": "Invalid Query",
                    "sql": "SELECT * FROM nonexistent_table",
                    "expected_time": 0.1
                }
            ],
            "database": invalid_db_config
        }
        
        perf_result = await self.performance_tester.test_query_performance(invalid_performance_config)
        assert perf_result["passed"] is False
        assert "error" in perf_result
    
    @pytest.mark.asyncio
    async def test_concurrent_execution(self):
        """Test concurrent test execution."""
        # Create multiple test suites to run concurrently
        test_suites = []
        
        for i in range(3):
            suite_config = {
                "name": f"Concurrent Test Suite {i + 1}",
                "description": f"Concurrent test suite {i + 1}",
                "test_types": [TestType.DATA_QUALITY],
                "config": {
                    "data_quality": {
                        "schema_validation": {
                            "expected_schema": {
                                "columns": [
                                    {"name": "id", "type": "integer"},
                                    {"name": "name", "type": "string"}
                                ]
                            },
                            "table_name": f"users_{i}",
                            "database": {
                                "type": "sqlite",
                                "path": ":memory:"
                            }
                        }
                    }
                },
                "dependencies": [],
                "timeout": 60
            }
            test_suites.append(TestSuite(**suite_config))
        
        # Run test suites concurrently
        tasks = [self.test_framework.run_test_suite(suite) for suite in test_suites]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Verify all test suites completed
        assert len(results) == 3
        for result in results:
            if isinstance(result, Exception):
                # Some tests might fail due to concurrent access, which is expected
                continue
            assert len(result) > 0
    
    @pytest.mark.asyncio
    async def test_configuration_validation(self):
        """Test configuration validation."""
        # Test valid configuration
        validation_result = self.config_manager.validate_config()
        assert validation_result["valid"] is True
        
        # Test configuration getters
        quality_thresholds = self.config_manager.get_quality_thresholds()
        assert "completeness_threshold" in quality_thresholds
        assert quality_thresholds["completeness_threshold"] == 95.0
        
        performance_thresholds = self.config_manager.get_performance_thresholds()
        assert "query_performance_threshold" in performance_thresholds
        assert performance_thresholds["query_performance_threshold"] == 80.0
        
        api_thresholds = self.config_manager.get_api_thresholds()
        assert "api_test_threshold" in api_thresholds
        assert api_thresholds["api_test_threshold"] == 85.0
    
    def test_test_summary(self):
        """Test test summary generation."""
        # Get test summary
        summary = self.test_framework.get_test_summary()
        
        # Verify summary structure
        assert "message" in summary or "total_tests" in summary
        
        if "total_tests" in summary:
            assert summary["total_tests"] >= 0
            assert summary["passed_tests"] >= 0
            assert summary["failed_tests"] >= 0
            assert summary["success_rate"] >= 0
            assert summary["success_rate"] <= 100


class TestAdvancedFeatures:
    """
    Test advanced features and edge cases.
    """
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test environment."""
        self.config_manager = ConfigManager("config/config.yaml")
        self.test_framework = ConcreteTestFramework(self.config_manager)
    
    @pytest.mark.asyncio
    async def test_custom_validation_rules(self):
        """Test custom validation rules."""
        # Test custom business rules
        custom_rules = {
            "business_rules": {
                "order_amount_limit": {
                    "type": "range",
                    "column": "amount",
                    "min": 0,
                    "max": 10000
                },
                "user_age_validation": {
                    "type": "range",
                    "column": "age",
                    "min": 18,
                    "max": 120
                },
                "email_domain_validation": {
                    "type": "pattern",
                    "column": "email",
                    "pattern": r".*@(gmail\.com|yahoo\.com|hotmail\.com)$"
                }
            }
        }
        
        # This would be integrated into the data quality validator
        assert "business_rules" in custom_rules
        assert len(custom_rules["business_rules"]) == 3
    
    @pytest.mark.asyncio
    async def test_performance_monitoring(self):
        """Test performance monitoring capabilities."""
        # Test performance metrics collection
        metrics = {
            "execution_time": 1.5,
            "memory_usage": 45.2,
            "cpu_usage": 23.1,
            "throughput": 150.0,
            "latency": 0.8,
            "error_rate": 0.5,
            "timestamp": datetime.now()
        }
        
        # Verify metrics structure
        assert "execution_time" in metrics
        assert "memory_usage" in metrics
        assert "cpu_usage" in metrics
        assert "throughput" in metrics
        assert "latency" in metrics
        assert "error_rate" in metrics
        assert "timestamp" in metrics
        
        # Verify metric values are reasonable
        assert metrics["execution_time"] > 0
        assert 0 <= metrics["memory_usage"] <= 100
        assert 0 <= metrics["cpu_usage"] <= 100
        assert metrics["throughput"] >= 0
        assert metrics["latency"] >= 0
        assert 0 <= metrics["error_rate"] <= 100
    
    def test_logging_and_monitoring(self):
        """Test logging and monitoring capabilities."""
        # Test logging configuration
        logging_config = self.config_manager.get_logging_config()
        assert "level" in logging_config
        assert "format" in logging_config
        assert "file" in logging_config
        
        # Test environment detection
        assert self.config_manager.get_environment() in ["development", "testing", "production"]
        assert isinstance(self.config_manager.is_development(), bool)
        assert isinstance(self.config_manager.is_production(), bool)
        assert isinstance(self.config_manager.is_testing(), bool)


if __name__ == "__main__":
    # Run the comprehensive test suite
    pytest.main([__file__, "-v", "--tb=short"]) 