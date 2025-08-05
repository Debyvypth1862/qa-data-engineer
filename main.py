#!/usr/bin/env python3
"""
Main Execution Script

This script demonstrates how to use the comprehensive framework
for testing data pipelines, APIs, and data quality validation.
"""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import Dict, Any, List
import logging

from src.core.test_framework import ConcreteTestFramework, TestSuite, TestType
from src.utils.config_manager import ConfigManager
from src.utils.database_connector import DatabaseConnector


def setup_logging(config_manager: ConfigManager) -> None:
    """Set up logging configuration."""
    logging_config = config_manager.get_logging_config()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, logging_config.get("level", "INFO")),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logging_config.get("file", "logs/qa_engineer.log")),
            logging.StreamHandler(sys.stdout)
        ]
    )


async def run_data_quality_tests(config_manager: ConfigManager) -> Dict[str, Any]:
    """Run comprehensive data quality tests."""
    print("🔍 Running Data Quality Tests...")
    
    test_framework = ConcreteTestFramework(config_manager)
    
    # Create test suite for data quality
    data_quality_suite = TestSuite(
        name="Data Quality Test Suite",
        description="Comprehensive data quality validation",
        test_types=[TestType.DATA_QUALITY],
        config={
            "data_quality": {
                "schema_validation": {
                    "expected_schema": {
                        "columns": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                            {"name": "email", "type": "string"},
                            {"name": "created_at", "type": "datetime"}
                        ]
                    },
                    "table_name": "users",
                    "database": config_manager.get_database_config()
                },
                "completeness_validation": {
                    "table_name": "users",
                    "database": config_manager.get_database_config(),
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
                },
                "accuracy_validation": {
                    "table_name": "users",
                    "database": config_manager.get_database_config(),
                    "accuracy_rules": {
                        "email_format": {
                            "type": "pattern",
                            "column": "email",
                            "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                        }
                    }
                }
            }
        },
        dependencies=["database"],
        timeout=300
    )
    
    results = await test_framework.run_test_suite(data_quality_suite)
    
    # Generate summary
    summary = test_framework.get_test_summary()
    
    print(f"✅ Data Quality Tests Completed")
    print(f"   Total Tests: {summary.get('total_tests', 0)}")
    print(f"   Passed: {summary.get('passed_tests', 0)}")
    print(f"   Failed: {summary.get('failed_tests', 0)}")
    print(f"   Success Rate: {summary.get('success_rate', 0):.2f}%")
    
    return {
        "test_type": "data_quality",
        "results": results,
        "summary": summary
    }


async def run_performance_tests(config_manager: ConfigManager) -> Dict[str, Any]:
    """Run comprehensive performance tests."""
    print("⚡ Running Performance Tests...")
    
    test_framework = ConcreteTestFramework(config_manager)
    
    # Create test suite for performance
    performance_suite = TestSuite(
        name="Performance Test Suite",
        description="Comprehensive performance validation",
        test_types=[TestType.PERFORMANCE],
        config={
            "performance": {
                "query_performance": {
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
                    "database": config_manager.get_database_config(),
                    "iterations": 5,
                    "concurrent_users": 3
                },
                "load_testing": {
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
                    "database": config_manager.get_database_config(),
                    "duration": 60
                }
            }
        },
        dependencies=["database"],
        timeout=600
    )
    
    results = await test_framework.run_test_suite(performance_suite)
    
    # Generate summary
    summary = test_framework.get_test_summary()
    
    print(f"✅ Performance Tests Completed")
    print(f"   Total Tests: {summary.get('total_tests', 0)}")
    print(f"   Passed: {summary.get('passed_tests', 0)}")
    print(f"   Failed: {summary.get('failed_tests', 0)}")
    print(f"   Success Rate: {summary.get('success_rate', 0):.2f}%")
    
    return {
        "test_type": "performance",
        "results": results,
        "summary": summary
    }


async def run_api_tests(config_manager: ConfigManager) -> Dict[str, Any]:
    """Run comprehensive API tests."""
    print("🌐 Running API Tests...")
    
    test_framework = ConcreteTestFramework(config_manager)
    
    # Create test suite for API testing
    api_suite = TestSuite(
        name="API Test Suite",
        description="Comprehensive API validation",
        test_types=[TestType.API],
        config={
            "api": {
                "base_url": config_manager.get_api_config().get("base_url", "http://localhost:8000"),
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
                "authentication": config_manager.get_api_config(),
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
        },
        dependencies=["api_server"],
        timeout=300
    )
    
    results = await test_framework.run_test_suite(api_suite)
    
    # Generate summary
    summary = test_framework.get_test_summary()
    
    print(f"✅ API Tests Completed")
    print(f"   Total Tests: {summary.get('total_tests', 0)}")
    print(f"   Passed: {summary.get('passed_tests', 0)}")
    print(f"   Failed: {summary.get('failed_tests', 0)}")
    print(f"   Success Rate: {summary.get('success_rate', 0):.2f}%")
    
    return {
        "test_type": "api",
        "results": results,
        "summary": summary
    }


async def run_integration_tests(config_manager: ConfigManager) -> Dict[str, Any]:
    """Run comprehensive integration tests."""
    print("🔗 Running Integration Tests...")
    
    test_framework = ConcreteTestFramework(config_manager)
    
    # Create test suite for integration testing
    integration_suite = TestSuite(
        name="Integration Test Suite",
        description="End-to-end integration validation",
        test_types=[TestType.DATA_QUALITY, TestType.PERFORMANCE, TestType.API],
        config={
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
                    "database": config_manager.get_database_config()
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
                    "database": config_manager.get_database_config(),
                    "iterations": 3,
                    "concurrent_users": 2
                }
            },
            "api": {
                "base_url": config_manager.get_api_config().get("base_url", "http://localhost:8000"),
                "endpoints": [
                    {
                        "name": "Health Check",
                        "path": "/health",
                        "method": "GET",
                        "expected_status": 200
                    }
                ],
                "authentication": config_manager.get_api_config()
            }
        },
        dependencies=["database", "api_server"],
        timeout=600
    )
    
    results = await test_framework.run_test_suite(integration_suite)
    
    # Generate summary
    summary = test_framework.get_test_summary()
    
    print(f"✅ Integration Tests Completed")
    print(f"   Total Tests: {summary.get('total_tests', 0)}")
    print(f"   Passed: {summary.get('passed_tests', 0)}")
    print(f"   Failed: {summary.get('failed_tests', 0)}")
    print(f"   Success Rate: {summary.get('success_rate', 0):.2f}%")
    
    return {
        "test_type": "integration",
        "results": results,
        "summary": summary
    }


async def run_all_tests(config_manager: ConfigManager) -> List[Dict[str, Any]]:
    """Run all test suites."""
    print("🚀 Starting Comprehensive Test Suite")
    print("=" * 60)
    
    all_results = []
    
    # Run data quality tests
    try:
        data_quality_results = await run_data_quality_tests(config_manager)
        all_results.append(data_quality_results)
    except Exception as e:
        print(f"❌ Data Quality Tests Failed: {str(e)}")
    
    # Run performance tests
    try:
        performance_results = await run_performance_tests(config_manager)
        all_results.append(performance_results)
    except Exception as e:
        print(f"❌ Performance Tests Failed: {str(e)}")
    
    # Run API tests
    try:
        api_results = await run_api_tests(config_manager)
        all_results.append(api_results)
    except Exception as e:
        print(f"❌ API Tests Failed: {str(e)}")
    
    # Run integration tests
    try:
        integration_results = await run_integration_tests(config_manager)
        all_results.append(integration_results)
    except Exception as e:
        print(f"❌ Integration Tests Failed: {str(e)}")
    
    # Generate overall summary
    print("\n" + "=" * 60)
    print("📊 Overall Test Summary")
    print("=" * 60)
    
    total_tests = 0
    total_passed = 0
    total_failed = 0
    
    for result in all_results:
        summary = result["summary"]
        if "total_tests" in summary:
            total_tests += summary["total_tests"]
            total_passed += summary["passed_tests"]
            total_failed += summary["failed_tests"]
    
    if total_tests > 0:
        overall_success_rate = (total_passed / total_tests) * 100
        print(f"   Total Tests: {total_tests}")
        print(f"   Passed: {total_passed}")
        print(f"   Failed: {total_failed}")
        print(f"   Overall Success Rate: {overall_success_rate:.2f}%")
        
        if overall_success_rate >= 90:
            print("   🎉 Excellent! All tests passed with high success rate.")
        elif overall_success_rate >= 80:
            print("   ✅ Good! Most tests passed.")
        elif overall_success_rate >= 70:
            print("   ⚠️  Warning! Some tests failed.")
        else:
            print("   ❌ Critical! Many tests failed.")
    else:
        print("   No tests were executed.")
    
    return all_results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description=" Test Suite")
    parser.add_argument(
        "--config", 
        default="config/config.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--test-type",
        choices=["data-quality", "performance", "api", "integration", "all"],
        default="all",
        help="Type of tests to run"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config_manager = ConfigManager(args.config)
        setup_logging(config_manager)
    except Exception as e:
        print(f"❌ Failed to load configuration: {str(e)}")
        sys.exit(1)
    
    # Validate configuration
    validation_result = config_manager.validate_config()
    if not validation_result["valid"]:
        print("❌ Configuration validation failed:")
        for error in validation_result["errors"]:
            print(f"   - {error}")
        sys.exit(1)
    
    if validation_result["warnings"]:
        print("⚠️  Configuration warnings:")
        for warning in validation_result["warnings"]:
            print(f"   - {warning}")
    
    # Run tests based on type
    try:
        if args.test_type == "data-quality":
            asyncio.run(run_data_quality_tests(config_manager))
        elif args.test_type == "performance":
            asyncio.run(run_performance_tests(config_manager))
        elif args.test_type == "api":
            asyncio.run(run_api_tests(config_manager))
        elif args.test_type == "integration":
            asyncio.run(run_integration_tests(config_manager))
        else:  # all
            asyncio.run(run_all_tests(config_manager))
        
        print("\n🎯  Test Suite completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Test execution interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test execution failed: {str(e)}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 