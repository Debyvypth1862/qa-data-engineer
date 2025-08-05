"""
Performance Testing Framework

This module provides comprehensive performance testing capabilities including
query performance testing, load testing, stress testing, and benchmarking.
"""

import asyncio
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import structlog
import psutil
import threading
from dataclasses import dataclass

from ..utils.config_manager import ConfigManager
from ..utils.database_connector import DatabaseConnector

logger = structlog.get_logger()


@dataclass
class PerformanceMetrics:
    """Data class for storing performance metrics."""
    execution_time: float
    memory_usage: float
    cpu_usage: float
    throughput: float
    latency: float
    error_rate: float
    timestamp: datetime


class PerformanceTester:
    """
    Comprehensive performance testing framework for data engineering systems.
    
    This class provides advanced performance testing capabilities including:
    - Query performance testing and optimization
    - Load testing with concurrent users
    - Stress testing to find system limits
    - Benchmark testing and comparison
    - Resource monitoring and analysis
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize the performance tester.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.db_connector = DatabaseConnector(config_manager)
        
        # Performance thresholds
        self.performance_thresholds = self.config_manager.get_config("performance_thresholds", {})
        
        # Metrics storage
        self.performance_metrics: List[PerformanceMetrics] = []
        
        logger.info("Performance tester initialized")
    
    async def test_query_performance(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test query performance and optimization.
        
        Args:
            config: Query performance testing configuration
            
        Returns:
            Performance test results
        """
        logger.info("Starting query performance testing")
        
        try:
            queries = config.get("queries", [])
            database_config = config.get("database", {})
            iterations = config.get("iterations", 10)
            concurrent_users = config.get("concurrent_users", 1)
            
            if not queries:
                return {
                    "passed": False,
                    "error": "No queries provided for testing",
                    "summary": "Query performance testing failed due to missing queries"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Test each query
            query_results = []
            for query_config in queries:
                query_result = await self._test_single_query(
                    engine, query_config, iterations, concurrent_users
                )
                query_results.append(query_result)
            
            # Calculate overall performance score
            overall_score = self._calculate_performance_score(query_results)
            
            test_result = {
                "passed": overall_score >= self.performance_thresholds.get("query_performance_threshold", 80),
                "overall_score": overall_score,
                "query_results": query_results,
                "metrics": {
                    "total_queries": len(queries),
                    "average_execution_time": statistics.mean([qr["average_execution_time"] for qr in query_results]),
                    "max_execution_time": max([qr["max_execution_time"] for qr in query_results]),
                    "min_execution_time": min([qr["min_execution_time"] for qr in query_results])
                },
                "summary": f"Query performance score: {overall_score:.2f}%"
            }
            
            logger.info("Query performance testing completed", 
                       passed=test_result["passed"],
                       overall_score=overall_score)
            
            return test_result
            
        except Exception as e:
            logger.error("Query performance testing failed", error=str(e))
            return {
                "passed": False,
                "error": str(e),
                "summary": f"Query performance testing failed: {str(e)}"
            }
    
    async def test_load_performance(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test system performance under load.
        
        Args:
            config: Load testing configuration
            
        Returns:
            Load test results
        """
        logger.info("Starting load performance testing")
        
        try:
            test_scenarios = config.get("scenarios", [])
            database_config = config.get("database", {})
            duration = config.get("duration", 300)  # seconds
            
            if not test_scenarios:
                return {
                    "passed": False,
                    "error": "No load test scenarios provided",
                    "summary": "Load testing failed due to missing scenarios"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Run load test scenarios
            scenario_results = []
            for scenario in test_scenarios:
                scenario_result = await self._run_load_scenario(
                    engine, scenario, duration
                )
                scenario_results.append(scenario_result)
            
            # Calculate overall load test score
            load_score = self._calculate_load_test_score(scenario_results)
            
            test_result = {
                "passed": load_score >= self.performance_thresholds.get("load_test_threshold", 85),
                "load_score": load_score,
                "scenario_results": scenario_results,
                "metrics": {
                    "total_scenarios": len(test_scenarios),
                    "average_throughput": statistics.mean([sr["throughput"] for sr in scenario_results]),
                    "average_latency": statistics.mean([sr["average_latency"] for sr in scenario_results]),
                    "max_concurrent_users": max([sr["concurrent_users"] for sr in scenario_results])
                },
                "summary": f"Load test score: {load_score:.2f}%"
            }
            
            logger.info("Load performance testing completed", 
                       passed=test_result["passed"],
                       load_score=load_score)
            
            return test_result
            
        except Exception as e:
            logger.error("Load performance testing failed", error=str(e))
            return {
                "passed": False,
                "error": str(e),
                "summary": f"Load testing failed: {str(e)}"
            }
    
    async def test_stress_performance(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Test system performance under stress conditions.
        
        Args:
            config: Stress testing configuration
            
        Returns:
            Stress test results
        """
        logger.info("Starting stress performance testing")
        
        try:
            stress_scenarios = config.get("scenarios", [])
            database_config = config.get("database", {})
            max_duration = config.get("max_duration", 600)  # seconds
            
            if not stress_scenarios:
                return {
                    "passed": False,
                    "error": "No stress test scenarios provided",
                    "summary": "Stress testing failed due to missing scenarios"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Run stress test scenarios
            stress_results = []
            for scenario in stress_scenarios:
                stress_result = await self._run_stress_scenario(
                    engine, scenario, max_duration
                )
                stress_results.append(stress_result)
            
            # Calculate stress test score
            stress_score = self._calculate_stress_test_score(stress_results)
            
            test_result = {
                "passed": stress_score >= self.performance_thresholds.get("stress_test_threshold", 70),
                "stress_score": stress_score,
                "stress_results": stress_results,
                "metrics": {
                    "total_scenarios": len(stress_scenarios),
                    "breaking_points": [sr["breaking_point"] for sr in stress_results if sr["breaking_point"]],
                    "recovery_times": [sr["recovery_time"] for sr in stress_results if sr["recovery_time"]]
                },
                "summary": f"Stress test score: {stress_score:.2f}%"
            }
            
            logger.info("Stress performance testing completed", 
                       passed=test_result["passed"],
                       stress_score=stress_score)
            
            return test_result
            
        except Exception as e:
            logger.error("Stress performance testing failed", error=str(e))
            return {
                "passed": False,
                "error": str(e),
                "summary": f"Stress testing failed: {str(e)}"
            }
    
    async def benchmark_performance(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run benchmark tests and compare with baselines.
        
        Args:
            config: Benchmark testing configuration
            
        Returns:
            Benchmark results
        """
        logger.info("Starting benchmark performance testing")
        
        try:
            benchmarks = config.get("benchmarks", [])
            database_config = config.get("database", {})
            baseline_config = config.get("baseline", {})
            
            if not benchmarks:
                return {
                    "passed": False,
                    "error": "No benchmark tests provided",
                    "summary": "Benchmark testing failed due to missing benchmarks"
                }
            
            # Connect to database
            engine = await self.db_connector.get_connection(database_config)
            
            # Run benchmark tests
            benchmark_results = []
            for benchmark in benchmarks:
                benchmark_result = await self._run_benchmark_test(
                    engine, benchmark, baseline_config
                )
                benchmark_results.append(benchmark_result)
            
            # Calculate overall benchmark score
            benchmark_score = self._calculate_benchmark_score(benchmark_results)
            
            test_result = {
                "passed": benchmark_score >= self.performance_thresholds.get("benchmark_threshold", 90),
                "benchmark_score": benchmark_score,
                "benchmark_results": benchmark_results,
                "metrics": {
                    "total_benchmarks": len(benchmarks),
                    "improvements": [br["improvement"] for br in benchmark_results if br["improvement"] > 0],
                    "regressions": [br["regression"] for br in benchmark_results if br["regression"] > 0]
                },
                "summary": f"Benchmark score: {benchmark_score:.2f}%"
            }
            
            logger.info("Benchmark performance testing completed", 
                       passed=test_result["passed"],
                       benchmark_score=benchmark_score)
            
            return test_result
            
        except Exception as e:
            logger.error("Benchmark performance testing failed", error=str(e))
            return {
                "passed": False,
                "error": str(e),
                "summary": f"Benchmark testing failed: {str(e)}"
            }
    
    async def _test_single_query(self, engine, query_config: Dict[str, Any], 
                                iterations: int, concurrent_users: int) -> Dict[str, Any]:
        """Test performance of a single query."""
        query_name = query_config.get("name", "Unknown Query")
        query_sql = query_config.get("sql")
        expected_time = query_config.get("expected_time", 1.0)  # seconds
        
        if not query_sql:
            return {
                "query_name": query_name,
                "error": "No SQL query provided",
                "passed": False
            }
        
        execution_times = []
        memory_usage = []
        cpu_usage = []
        
        # Run query multiple times
        for i in range(iterations):
            start_time = time.time()
            start_memory = psutil.virtual_memory().percent
            start_cpu = psutil.cpu_percent()
            
            try:
                with engine.connect() as connection:
                    result = connection.execute(text(query_sql))
                    # Fetch all results to ensure query completes
                    result.fetchall()
                
                end_time = time.time()
                end_memory = psutil.virtual_memory().percent
                end_cpu = psutil.cpu_percent()
                
                execution_time = end_time - start_time
                execution_times.append(execution_time)
                memory_usage.append((start_memory + end_memory) / 2)
                cpu_usage.append((start_cpu + end_cpu) / 2)
                
            except SQLAlchemyError as e:
                logger.error(f"Query execution failed: {str(e)}")
                execution_times.append(float('inf'))
        
        # Calculate statistics
        avg_execution_time = statistics.mean(execution_times) if execution_times else 0
        max_execution_time = max(execution_times) if execution_times else 0
        min_execution_time = min(execution_times) if execution_times else 0
        
        # Determine if query meets performance requirements
        passed = avg_execution_time <= expected_time
        
        return {
            "query_name": query_name,
            "passed": passed,
            "average_execution_time": avg_execution_time,
            "max_execution_time": max_execution_time,
            "min_execution_time": min_execution_time,
            "expected_time": expected_time,
            "iterations": iterations,
            "memory_usage": statistics.mean(memory_usage) if memory_usage else 0,
            "cpu_usage": statistics.mean(cpu_usage) if cpu_usage else 0,
            "execution_times": execution_times
        }
    
    async def _run_load_scenario(self, engine, scenario: Dict[str, Any], 
                                duration: int) -> Dict[str, Any]:
        """Run a single load test scenario."""
        scenario_name = scenario.get("name", "Unknown Scenario")
        concurrent_users = scenario.get("concurrent_users", 10)
        queries = scenario.get("queries", [])
        
        if not queries:
            return {
                "scenario_name": scenario_name,
                "error": "No queries provided for load scenario",
                "passed": False
            }
        
        start_time = time.time()
        end_time = start_time + duration
        
        # Metrics collection
        response_times = []
        throughput_metrics = []
        error_count = 0
        success_count = 0
        
        # Create thread pool for concurrent execution
        with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            futures = []
            
            while time.time() < end_time:
                # Submit queries to thread pool
                for query in queries:
                    future = executor.submit(self._execute_query_under_load, engine, query)
                    futures.append(future)
                
                # Wait for a batch to complete
                for future in as_completed(futures[:concurrent_users]):
                    try:
                        result = future.result(timeout=30)
                        if result["success"]:
                            response_times.append(result["execution_time"])
                            success_count += 1
                        else:
                            error_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Load test query failed: {str(e)}")
                
                # Calculate current throughput
                elapsed_time = time.time() - start_time
                current_throughput = success_count / elapsed_time if elapsed_time > 0 else 0
                throughput_metrics.append(current_throughput)
        
        # Calculate final metrics
        total_requests = success_count + error_count
        error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0
        average_latency = statistics.mean(response_times) if response_times else 0
        throughput = success_count / duration if duration > 0 else 0
        
        return {
            "scenario_name": scenario_name,
            "concurrent_users": concurrent_users,
            "duration": duration,
            "passed": error_rate <= 5 and average_latency <= 2.0,  # 5% error rate, 2s latency
            "throughput": throughput,
            "average_latency": average_latency,
            "error_rate": error_rate,
            "total_requests": total_requests,
            "success_count": success_count,
            "error_count": error_count,
            "response_times": response_times
        }
    
    async def _run_stress_scenario(self, engine, scenario: Dict[str, Any], 
                                  max_duration: int) -> Dict[str, Any]:
        """Run a single stress test scenario."""
        scenario_name = scenario.get("name", "Unknown Stress Scenario")
        initial_users = scenario.get("initial_users", 10)
        max_users = scenario.get("max_users", 100)
        user_increment = scenario.get("user_increment", 10)
        increment_interval = scenario.get("increment_interval", 30)  # seconds
        
        queries = scenario.get("queries", [])
        if not queries:
            return {
                "scenario_name": scenario_name,
                "error": "No queries provided for stress scenario",
                "passed": False
            }
        
        current_users = initial_users
        breaking_point = None
        recovery_time = None
        
        start_time = time.time()
        
        while current_users <= max_users and time.time() - start_time < max_duration:
            # Run load test with current number of users
            load_result = await self._run_load_scenario(
                engine, 
                {"name": f"{scenario_name}_{current_users}", "concurrent_users": current_users, "queries": queries},
                increment_interval
            )
            
            # Check if system is stressed
            if load_result["error_rate"] > 10 or load_result["average_latency"] > 5.0:
                breaking_point = current_users
                # Test recovery
                recovery_time = await self._test_recovery(engine, scenario, current_users - user_increment)
                break
            
            current_users += user_increment
        
        return {
            "scenario_name": scenario_name,
            "max_users_tested": current_users,
            "breaking_point": breaking_point,
            "recovery_time": recovery_time,
            "passed": breaking_point is not None and recovery_time is not None,
            "final_error_rate": load_result["error_rate"] if 'load_result' in locals() else 0,
            "final_latency": load_result["average_latency"] if 'load_result' in locals() else 0
        }
    
    async def _run_benchmark_test(self, engine, benchmark: Dict[str, Any], 
                                 baseline_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single benchmark test."""
        benchmark_name = benchmark.get("name", "Unknown Benchmark")
        queries = benchmark.get("queries", [])
        baseline_metrics = baseline_config.get(benchmark_name, {})
        
        if not queries:
            return {
                "benchmark_name": benchmark_name,
                "error": "No queries provided for benchmark",
                "passed": False
            }
        
        # Run benchmark queries
        benchmark_metrics = {}
        for query in queries:
            query_result = await self._test_single_query(engine, query, iterations=5, concurrent_users=1)
            benchmark_metrics[query["name"]] = query_result["average_execution_time"]
        
        # Compare with baseline
        improvements = []
        regressions = []
        
        for query_name, current_time in benchmark_metrics.items():
            baseline_time = baseline_metrics.get(query_name, current_time)
            
            if baseline_time > 0:
                improvement = ((baseline_time - current_time) / baseline_time) * 100
                if improvement > 0:
                    improvements.append(improvement)
                else:
                    regressions.append(abs(improvement))
        
        overall_improvement = statistics.mean(improvements) if improvements else 0
        overall_regression = statistics.mean(regressions) if regressions else 0
        
        return {
            "benchmark_name": benchmark_name,
            "passed": overall_improvement >= 0,  # No regression
            "improvement": overall_improvement,
            "regression": overall_regression,
            "benchmark_metrics": benchmark_metrics,
            "baseline_metrics": baseline_metrics,
            "improvements": improvements,
            "regressions": regressions
        }
    
    def _execute_query_under_load(self, engine, query: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single query under load testing conditions."""
        query_sql = query.get("sql")
        
        start_time = time.time()
        
        try:
            with engine.connect() as connection:
                result = connection.execute(text(query_sql))
                result.fetchall()
            
            execution_time = time.time() - start_time
            
            return {
                "success": True,
                "execution_time": execution_time
            }
            
        except Exception as e:
            return {
                "success": False,
                "execution_time": time.time() - start_time,
                "error": str(e)
            }
    
    async def _test_recovery(self, engine, scenario: Dict[str, Any], 
                            recovery_users: int) -> Optional[float]:
        """Test system recovery after stress."""
        recovery_queries = scenario.get("recovery_queries", scenario.get("queries", []))
        
        if not recovery_queries:
            return None
        
        start_time = time.time()
        
        # Run recovery test with reduced load
        recovery_result = await self._run_load_scenario(
            engine,
            {"name": "recovery_test", "concurrent_users": recovery_users, "queries": recovery_queries},
            60  # 1 minute recovery test
        )
        
        recovery_time = time.time() - start_time
        
        # Check if system recovered
        if recovery_result["error_rate"] <= 5 and recovery_result["average_latency"] <= 2.0:
            return recovery_time
        
        return None
    
    def _calculate_performance_score(self, query_results: List[Dict[str, Any]]) -> float:
        """Calculate overall performance score from query results."""
        if not query_results:
            return 0.0
        
        scores = []
        for result in query_results:
            if result.get("passed", False):
                # Calculate score based on how much better than expected
                expected_time = result.get("expected_time", 1.0)
                actual_time = result.get("average_execution_time", 0)
                
                if expected_time > 0:
                    score = min(100, (expected_time / actual_time) * 100) if actual_time > 0 else 100
                    scores.append(score)
                else:
                    scores.append(100)
            else:
                scores.append(0)
        
        return statistics.mean(scores) if scores else 0.0
    
    def _calculate_load_test_score(self, scenario_results: List[Dict[str, Any]]) -> float:
        """Calculate overall load test score."""
        if not scenario_results:
            return 0.0
        
        scores = []
        for result in scenario_results:
            if result.get("passed", False):
                # Score based on throughput and error rate
                throughput_score = min(100, result.get("throughput", 0) * 10)  # Normalize throughput
                error_score = max(0, 100 - result.get("error_rate", 0) * 10)  # Penalize errors
                latency_score = max(0, 100 - result.get("average_latency", 0) * 20)  # Penalize latency
                
                score = (throughput_score + error_score + latency_score) / 3
                scores.append(score)
            else:
                scores.append(0)
        
        return statistics.mean(scores) if scores else 0.0
    
    def _calculate_stress_test_score(self, stress_results: List[Dict[str, Any]]) -> float:
        """Calculate overall stress test score."""
        if not stress_results:
            return 0.0
        
        scores = []
        for result in stress_results:
            if result.get("passed", False):
                # Score based on breaking point and recovery
                breaking_point = result.get("breaking_point", 0)
                recovery_time = result.get("recovery_time", 0)
                
                breaking_score = min(100, breaking_point)  # Higher breaking point = better
                recovery_score = max(0, 100 - recovery_time)  # Lower recovery time = better
                
                score = (breaking_score + recovery_score) / 2
                scores.append(score)
            else:
                scores.append(0)
        
        return statistics.mean(scores) if scores else 0.0
    
    def _calculate_benchmark_score(self, benchmark_results: List[Dict[str, Any]]) -> float:
        """Calculate overall benchmark score."""
        if not benchmark_results:
            return 0.0
        
        scores = []
        for result in benchmark_results:
            if result.get("passed", False):
                # Score based on improvement over baseline
                improvement = result.get("improvement", 0)
                regression = result.get("regression", 0)
                
                score = 100 + improvement - regression
                scores.append(max(0, score))
            else:
                scores.append(0)
        
        return statistics.mean(scores) if scores else 0.0 