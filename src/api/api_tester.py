"""
API Testing Framework

This module provides comprehensive API testing capabilities including
RESTful API testing, data contract validation, load testing, and security testing.
"""

import asyncio
import json
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
import httpx
import requests
from requests.auth import HTTPBasicAuth, HTTPDigestAuth
import jwt
import hashlib
import hmac
import structlog
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

from ..utils.config_manager import ConfigManager
from ..utils.database_connector import DatabaseConnector

logger = structlog.get_logger()


@dataclass
class APITestResult:
    """Data class for storing API test results."""
    endpoint: str
    method: str
    status_code: int
    response_time: float
    response_size: int
    success: bool
    error_message: Optional[str] = None
    response_data: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None


class APITester:
    """
    Comprehensive API testing framework for data engineering APIs.
    
    This class provides advanced API testing capabilities including:
    - RESTful API endpoint testing
    - Data contract validation
    - Authentication and authorization testing
    - Load testing for APIs
    - Security testing and vulnerability assessment
    - Response validation and schema checking
    """
    
    def __init__(self, config_manager: ConfigManager):
        """
        Initialize the API tester.
        
        Args:
            config_manager: Configuration manager instance
        """
        self.config_manager = config_manager
        self.db_connector = DatabaseConnector(config_manager)
        
        # API testing thresholds
        self.api_thresholds = self.config_manager.get_config("api_thresholds", {})
        
        # Test results storage
        self.test_results: List[APITestResult] = []
        
        # HTTP client configuration
        self.timeout = 30
        self.max_retries = 3
        
        logger.info("API tester initialized")
    
    async def run_api_tests(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run comprehensive API tests.
        
        Args:
            config: API testing configuration
            
        Returns:
            API test results
        """
        logger.info("Starting API testing")
        
        try:
            base_url = config.get("base_url")
            endpoints = config.get("endpoints", [])
            auth_config = config.get("authentication", {})
            test_scenarios = config.get("scenarios", [])
            
            if not base_url or not endpoints:
                return {
                    "passed": False,
                    "error": "Missing base_url or endpoints in config",
                    "summary": "API testing failed due to missing configuration"
                }
            
            # Run different types of API tests
            test_results = {}
            
            # Basic endpoint testing
            endpoint_results = await self._test_endpoints(base_url, endpoints, auth_config)
            test_results["endpoint_tests"] = endpoint_results
            
            # Data contract testing
            contract_results = await self._test_data_contracts(base_url, endpoints, auth_config)
            test_results["contract_tests"] = contract_results
            
            # Authentication testing
            auth_results = await self._test_authentication(base_url, endpoints, auth_config)
            test_results["auth_tests"] = auth_results
            
            # Load testing
            load_results = await self._test_api_load(base_url, test_scenarios, auth_config)
            test_results["load_tests"] = load_results
            
            # Security testing
            security_results = await self._test_api_security(base_url, endpoints, auth_config)
            test_results["security_tests"] = security_results
            
            # Calculate overall API test score
            overall_score = self._calculate_api_test_score(test_results)
            
            api_test_result = {
                "passed": overall_score >= self.api_thresholds.get("api_test_threshold", 85),
                "overall_score": overall_score,
                "test_results": test_results,
                "metrics": {
                    "total_endpoints": len(endpoints),
                    "total_scenarios": len(test_scenarios),
                    "average_response_time": self._calculate_average_response_time(),
                    "success_rate": self._calculate_success_rate()
                },
                "summary": f"API test score: {overall_score:.2f}%"
            }
            
            logger.info("API testing completed", 
                       passed=api_test_result["passed"],
                       overall_score=overall_score)
            
            return api_test_result
            
        except Exception as e:
            logger.error("API testing failed", error=str(e))
            return {
                "passed": False,
                "error": str(e),
                "summary": f"API testing failed: {str(e)}"
            }
    
    async def _test_endpoints(self, base_url: str, endpoints: List[Dict[str, Any]], 
                             auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test basic API endpoints."""
        logger.info("Testing API endpoints")
        
        endpoint_results = {}
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            method = endpoint_config.get("method", "GET")
            expected_status = endpoint_config.get("expected_status", 200)
            headers = endpoint_config.get("headers", {})
            params = endpoint_config.get("params", {})
            data = endpoint_config.get("data", {})
            
            full_url = urljoin(base_url, path)
            
            try:
                # Prepare authentication
                auth_headers = await self._prepare_auth_headers(auth_config, full_url, method)
                headers.update(auth_headers)
                
                # Make API request
                start_time = time.time()
                
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if method.upper() == "GET":
                        response = await client.get(full_url, headers=headers, params=params)
                    elif method.upper() == "POST":
                        response = await client.post(full_url, headers=headers, json=data, params=params)
                    elif method.upper() == "PUT":
                        response = await client.put(full_url, headers=headers, json=data, params=params)
                    elif method.upper() == "DELETE":
                        response = await client.delete(full_url, headers=headers, params=params)
                    else:
                        raise ValueError(f"Unsupported HTTP method: {method}")
                
                response_time = time.time() - start_time
                
                # Validate response
                success = response.status_code == expected_status
                
                result = APITestResult(
                    endpoint=full_url,
                    method=method,
                    status_code=response.status_code,
                    response_time=response_time,
                    response_size=len(response.content),
                    success=success,
                    response_data=response.json() if response.headers.get("content-type", "").startswith("application/json") else None,
                    headers=dict(response.headers)
                )
                
                if not success:
                    result.error_message = f"Expected status {expected_status}, got {response.status_code}"
                
                endpoint_results[endpoint_name] = {
                    "passed": success,
                    "result": result,
                    "expected_status": expected_status,
                    "actual_status": response.status_code,
                    "response_time": response_time
                }
                
                self.test_results.append(result)
                
            except Exception as e:
                logger.error(f"Endpoint test failed for {endpoint_name}", error=str(e))
                result = APITestResult(
                    endpoint=full_url,
                    method=method,
                    status_code=0,
                    response_time=0,
                    response_size=0,
                    success=False,
                    error_message=str(e)
                )
                
                endpoint_results[endpoint_name] = {
                    "passed": False,
                    "result": result,
                    "error": str(e)
                }
                
                self.test_results.append(result)
        
        return {
            "total_endpoints": len(endpoints),
            "passed_endpoints": len([r for r in endpoint_results.values() if r["passed"]]),
            "failed_endpoints": len([r for r in endpoint_results.values() if not r["passed"]]),
            "results": endpoint_results
        }
    
    async def _test_data_contracts(self, base_url: str, endpoints: List[Dict[str, Any]], 
                                  auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test data contract validation."""
        logger.info("Testing data contracts")
        
        contract_results = {}
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            method = endpoint_config.get("method", "GET")
            expected_schema = endpoint_config.get("expected_schema", {})
            validation_rules = endpoint_config.get("validation_rules", {})
            
            if not expected_schema and not validation_rules:
                continue
            
            full_url = urljoin(base_url, path)
            
            try:
                # Make API request
                auth_headers = await self._prepare_auth_headers(auth_config, full_url, method)
                
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if method.upper() == "GET":
                        response = await client.get(full_url, headers=auth_headers)
                    elif method.upper() == "POST":
                        response = await client.post(full_url, headers=auth_headers)
                    else:
                        continue
                
                # Validate response schema
                validation_results = []
                
                if response.status_code == 200 and expected_schema:
                    schema_validation = self._validate_response_schema(
                        response.json(), expected_schema
                    )
                    validation_results.append(schema_validation)
                
                # Apply custom validation rules
                if validation_rules and response.status_code == 200:
                    custom_validation = self._apply_validation_rules(
                        response.json(), validation_rules
                    )
                    validation_results.extend(custom_validation)
                
                # Calculate contract validation score
                passed_validations = len([v for v in validation_results if v["passed"]])
                total_validations = len(validation_results)
                contract_score = (passed_validations / total_validations) * 100 if total_validations > 0 else 100
                
                contract_results[endpoint_name] = {
                    "passed": contract_score >= 90,  # 90% validation success required
                    "contract_score": contract_score,
                    "validation_results": validation_results,
                    "passed_validations": passed_validations,
                    "total_validations": total_validations
                }
                
            except Exception as e:
                logger.error(f"Data contract test failed for {endpoint_name}", error=str(e))
                contract_results[endpoint_name] = {
                    "passed": False,
                    "error": str(e),
                    "contract_score": 0
                }
        
        return {
            "total_contracts": len(contract_results),
            "passed_contracts": len([r for r in contract_results.values() if r["passed"]]),
            "failed_contracts": len([r for r in contract_results.values() if not r["passed"]]),
            "results": contract_results
        }
    
    async def _test_authentication(self, base_url: str, endpoints: List[Dict[str, Any]], 
                                  auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test authentication and authorization."""
        logger.info("Testing authentication")
        
        auth_results = {}
        
        # Test with valid authentication
        valid_auth_results = await self._test_with_auth(base_url, endpoints, auth_config, "valid")
        auth_results["valid_auth"] = valid_auth_results
        
        # Test with invalid authentication
        invalid_auth_results = await self._test_with_auth(base_url, endpoints, auth_config, "invalid")
        auth_results["invalid_auth"] = invalid_auth_results
        
        # Test without authentication
        no_auth_results = await self._test_with_auth(base_url, endpoints, auth_config, "none")
        auth_results["no_auth"] = no_auth_results
        
        # Calculate authentication score
        auth_score = self._calculate_auth_score(auth_results)
        
        return {
            "auth_score": auth_score,
            "passed": auth_score >= 90,  # 90% authentication success required
            "results": auth_results
        }
    
    async def _test_api_load(self, base_url: str, test_scenarios: List[Dict[str, Any]], 
                            auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test API performance under load."""
        logger.info("Testing API load performance")
        
        load_results = {}
        
        for scenario in test_scenarios:
            scenario_name = scenario.get("name", "Unknown Scenario")
            endpoint = scenario.get("endpoint", "")
            method = scenario.get("method", "GET")
            concurrent_users = scenario.get("concurrent_users", 10)
            duration = scenario.get("duration", 60)  # seconds
            requests_per_second = scenario.get("requests_per_second", 10)
            
            full_url = urljoin(base_url, endpoint)
            
            try:
                # Run load test
                load_result = await self._run_api_load_test(
                    full_url, method, concurrent_users, duration, requests_per_second, auth_config
                )
                
                load_results[scenario_name] = load_result
                
            except Exception as e:
                logger.error(f"Load test failed for {scenario_name}", error=str(e))
                load_results[scenario_name] = {
                    "passed": False,
                    "error": str(e)
                }
        
        return {
            "total_scenarios": len(test_scenarios),
            "passed_scenarios": len([r for r in load_results.values() if r.get("passed", False)]),
            "failed_scenarios": len([r for r in load_results.values() if not r.get("passed", False)]),
            "results": load_results
        }
    
    async def _test_api_security(self, base_url: str, endpoints: List[Dict[str, Any]], 
                                auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test API security vulnerabilities."""
        logger.info("Testing API security")
        
        security_results = {}
        
        # Test for common security vulnerabilities
        security_tests = [
            ("sql_injection", self._test_sql_injection),
            ("xss", self._test_xss),
            ("authentication_bypass", self._test_auth_bypass),
            ("rate_limiting", self._test_rate_limiting),
            ("input_validation", self._test_input_validation)
        ]
        
        for test_name, test_func in security_tests:
            try:
                test_result = await test_func(base_url, endpoints, auth_config)
                security_results[test_name] = test_result
            except Exception as e:
                logger.error(f"Security test {test_name} failed", error=str(e))
                security_results[test_name] = {
                    "passed": False,
                    "error": str(e)
                }
        
        # Calculate security score
        security_score = self._calculate_security_score(security_results)
        
        return {
            "security_score": security_score,
            "passed": security_score >= 95,  # 95% security success required
            "results": security_results
        }
    
    async def _prepare_auth_headers(self, auth_config: Dict[str, Any], url: str, method: str) -> Dict[str, str]:
        """Prepare authentication headers based on configuration."""
        headers = {}
        
        auth_type = auth_config.get("type", "none")
        
        if auth_type == "bearer":
            token = auth_config.get("token")
            if token:
                headers["Authorization"] = f"Bearer {token}"
        
        elif auth_type == "basic":
            username = auth_config.get("username")
            password = auth_config.get("password")
            if username and password:
                import base64
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers["Authorization"] = f"Basic {credentials}"
        
        elif auth_type == "api_key":
            api_key = auth_config.get("api_key")
            header_name = auth_config.get("header_name", "X-API-Key")
            if api_key:
                headers[header_name] = api_key
        
        elif auth_type == "jwt":
            jwt_token = auth_config.get("jwt_token")
            if jwt_token:
                headers["Authorization"] = f"Bearer {jwt_token}"
        
        elif auth_type == "hmac":
            secret_key = auth_config.get("secret_key")
            if secret_key:
                # Generate HMAC signature
                timestamp = str(int(time.time()))
                message = f"{method}{url}{timestamp}"
                signature = hmac.new(
                    secret_key.encode(),
                    message.encode(),
                    hashlib.sha256
                ).hexdigest()
                
                headers["X-Timestamp"] = timestamp
                headers["X-Signature"] = signature
        
        return headers
    
    def _validate_response_schema(self, response_data: Any, expected_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Validate response against expected schema."""
        try:
            # Basic schema validation
            if isinstance(expected_schema, dict):
                if not isinstance(response_data, dict):
                    return {
                        "passed": False,
                        "error": "Response is not a dictionary",
                        "expected": "dict",
                        "actual": type(response_data).__name__
                    }
                
                # Check required fields
                required_fields = expected_schema.get("required", [])
                missing_fields = [field for field in required_fields if field not in response_data]
                
                if missing_fields:
                    return {
                        "passed": False,
                        "error": f"Missing required fields: {missing_fields}",
                        "missing_fields": missing_fields
                    }
                
                # Check field types
                field_types = expected_schema.get("properties", {})
                type_errors = []
                
                for field_name, expected_type in field_types.items():
                    if field_name in response_data:
                        actual_value = response_data[field_name]
                        if not self._check_field_type(actual_value, expected_type):
                            type_errors.append({
                                "field": field_name,
                                "expected": expected_type,
                                "actual": type(actual_value).__name__
                            })
                
                if type_errors:
                    return {
                        "passed": False,
                        "error": "Type validation failed",
                        "type_errors": type_errors
                    }
            
            return {
                "passed": True,
                "message": "Schema validation passed"
            }
            
        except Exception as e:
            return {
                "passed": False,
                "error": f"Schema validation error: {str(e)}"
            }
    
    def _check_field_type(self, value: Any, expected_type: str) -> bool:
        """Check if a value matches the expected type."""
        if expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "integer":
            return isinstance(value, int)
        elif expected_type == "number":
            return isinstance(value, (int, float))
        elif expected_type == "boolean":
            return isinstance(value, bool)
        elif expected_type == "array":
            return isinstance(value, list)
        elif expected_type == "object":
            return isinstance(value, dict)
        else:
            return True  # Unknown type, assume valid
    
    def _apply_validation_rules(self, response_data: Any, validation_rules: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply custom validation rules to response data."""
        validation_results = []
        
        for rule_name, rule_config in validation_rules.items():
            rule_type = rule_config.get("type")
            field_path = rule_config.get("field")
            expected_value = rule_config.get("expected")
            
            try:
                # Extract field value using path
                field_value = self._extract_field_value(response_data, field_path)
                
                if rule_type == "equals":
                    passed = field_value == expected_value
                elif rule_type == "not_equals":
                    passed = field_value != expected_value
                elif rule_type == "contains":
                    passed = expected_value in str(field_value)
                elif rule_type == "regex":
                    import re
                    passed = bool(re.match(expected_value, str(field_value)))
                elif rule_type == "range":
                    min_val = rule_config.get("min")
                    max_val = rule_config.get("max")
                    passed = (min_val is None or field_value >= min_val) and (max_val is None or field_value <= max_val)
                else:
                    passed = True
                
                validation_results.append({
                    "rule_name": rule_name,
                    "passed": passed,
                    "field_path": field_path,
                    "expected": expected_value,
                    "actual": field_value
                })
                
            except Exception as e:
                validation_results.append({
                    "rule_name": rule_name,
                    "passed": False,
                    "error": str(e)
                })
        
        return validation_results
    
    def _extract_field_value(self, data: Any, field_path: str) -> Any:
        """Extract field value using dot notation path."""
        if not field_path:
            return data
        
        keys = field_path.split(".")
        current = data
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            elif isinstance(current, list) and key.isdigit():
                current = current[int(key)]
            else:
                raise KeyError(f"Field path '{field_path}' not found in data")
        
        return current
    
    async def _test_with_auth(self, base_url: str, endpoints: List[Dict[str, Any]], 
                             auth_config: Dict[str, Any], auth_type: str) -> Dict[str, Any]:
        """Test endpoints with different authentication types."""
        results = {}
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            method = endpoint_config.get("method", "GET")
            expected_status = endpoint_config.get("expected_status", 200)
            
            full_url = urljoin(base_url, path)
            
            try:
                # Prepare authentication based on type
                if auth_type == "valid":
                    auth_headers = await self._prepare_auth_headers(auth_config, full_url, method)
                elif auth_type == "invalid":
                    # Use invalid credentials
                    invalid_auth = auth_config.copy()
                    invalid_auth["token"] = "invalid_token"
                    auth_headers = await self._prepare_auth_headers(invalid_auth, full_url, method)
                else:  # no auth
                    auth_headers = {}
                
                # Make request
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if method.upper() == "GET":
                        response = await client.get(full_url, headers=auth_headers)
                    else:
                        response = await client.get(full_url, headers=auth_headers)
                
                # Determine expected behavior
                if auth_type == "valid":
                    expected_behavior = response.status_code == expected_status
                elif auth_type == "invalid":
                    expected_behavior = response.status_code in [401, 403]  # Unauthorized/Forbidden
                else:  # no auth
                    expected_behavior = response.status_code in [401, 403]  # Should require auth
                
                results[endpoint_name] = {
                    "passed": expected_behavior,
                    "status_code": response.status_code,
                    "expected_behavior": expected_behavior
                }
                
            except Exception as e:
                results[endpoint_name] = {
                    "passed": False,
                    "error": str(e)
                }
        
        return results
    
    async def _run_api_load_test(self, url: str, method: str, concurrent_users: int, 
                                duration: int, requests_per_second: int, 
                                auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Run API load test."""
        start_time = time.time()
        end_time = start_time + duration
        
        response_times = []
        success_count = 0
        error_count = 0
        
        auth_headers = await self._prepare_auth_headers(auth_config, url, method)
        
        async def make_request():
            try:
                request_start = time.time()
                
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if method.upper() == "GET":
                        response = await client.get(url, headers=auth_headers)
                    elif method.upper() == "POST":
                        response = await client.post(url, headers=auth_headers)
                    else:
                        response = await client.get(url, headers=auth_headers)
                
                request_time = time.time() - request_start
                response_times.append(request_time)
                
                if response.status_code < 400:
                    return True
                else:
                    return False
                    
            except Exception as e:
                logger.error(f"Load test request failed: {str(e)}")
                return False
        
        # Run concurrent requests
        tasks = []
        while time.time() < end_time:
            # Create batch of requests
            batch_tasks = [make_request() for _ in range(concurrent_users)]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, bool):
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                else:
                    error_count += 1
            
            # Rate limiting
            await asyncio.sleep(1 / requests_per_second)
        
        # Calculate metrics
        total_requests = success_count + error_count
        error_rate = (error_count / total_requests) * 100 if total_requests > 0 else 0
        avg_response_time = statistics.mean(response_times) if response_times else 0
        throughput = total_requests / duration if duration > 0 else 0
        
        return {
            "passed": error_rate <= 5 and avg_response_time <= 2.0,  # 5% error rate, 2s latency
            "total_requests": total_requests,
            "success_count": success_count,
            "error_count": error_count,
            "error_rate": error_rate,
            "average_response_time": avg_response_time,
            "throughput": throughput,
            "concurrent_users": concurrent_users,
            "duration": duration
        }
    
    async def _test_sql_injection(self, base_url: str, endpoints: List[Dict[str, Any]], 
                                 auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test for SQL injection vulnerabilities."""
        sql_payloads = [
            "' OR '1'='1",
            "'; DROP TABLE users; --",
            "' UNION SELECT * FROM users --",
            "1' AND 1=1 --",
            "1' AND 1=2 --"
        ]
        
        vulnerable_endpoints = []
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            
            if "?" in path:  # Only test endpoints with parameters
                full_url = urljoin(base_url, path)
                
                for payload in sql_payloads:
                    try:
                        test_url = f"{full_url}&test={payload}"
                        auth_headers = await self._prepare_auth_headers(auth_config, test_url, "GET")
                        
                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(test_url, headers=auth_headers)
                        
                        # Check for SQL error indicators
                        response_text = response.text.lower()
                        sql_errors = ["sql", "mysql", "postgresql", "oracle", "syntax error", "database error"]
                        
                        if any(error in response_text for error in sql_errors):
                            vulnerable_endpoints.append({
                                "endpoint": endpoint_name,
                                "payload": payload,
                                "response_status": response.status_code
                            })
                            break
                    
                    except Exception as e:
                        logger.error(f"SQL injection test failed for {endpoint_name}", error=str(e))
        
        return {
            "passed": len(vulnerable_endpoints) == 0,
            "vulnerable_endpoints": vulnerable_endpoints,
            "total_endpoints_tested": len([e for e in endpoints if "?" in e.get("path", "")])
        }
    
    async def _test_xss(self, base_url: str, endpoints: List[Dict[str, Any]], 
                       auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test for XSS vulnerabilities."""
        xss_payloads = [
            "<script>alert('XSS')</script>",
            "javascript:alert('XSS')",
            "<img src=x onerror=alert('XSS')>",
            "';alert('XSS');//"
        ]
        
        vulnerable_endpoints = []
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            
            if "?" in path:
                full_url = urljoin(base_url, path)
                
                for payload in xss_payloads:
                    try:
                        test_url = f"{full_url}&test={payload}"
                        auth_headers = await self._prepare_auth_headers(auth_config, test_url, "GET")
                        
                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(test_url, headers=auth_headers)
                        
                        # Check if payload is reflected in response
                        if payload in response.text:
                            vulnerable_endpoints.append({
                                "endpoint": endpoint_name,
                                "payload": payload
                            })
                            break
                    
                    except Exception as e:
                        logger.error(f"XSS test failed for {endpoint_name}", error=str(e))
        
        return {
            "passed": len(vulnerable_endpoints) == 0,
            "vulnerable_endpoints": vulnerable_endpoints,
            "total_endpoints_tested": len([e for e in endpoints if "?" in e.get("path", "")])
        }
    
    async def _test_auth_bypass(self, base_url: str, endpoints: List[Dict[str, Any]], 
                               auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test for authentication bypass vulnerabilities."""
        bypass_attempts = []
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            method = endpoint_config.get("method", "GET")
            
            full_url = urljoin(base_url, path)
            
            try:
                # Try to access without authentication
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if method.upper() == "GET":
                        response = await client.get(full_url)
                    else:
                        response = await client.get(full_url)
                
                # Check if access was granted without auth
                if response.status_code == 200:
                    bypass_attempts.append({
                        "endpoint": endpoint_name,
                        "status_code": response.status_code
                    })
            
            except Exception as e:
                logger.error(f"Auth bypass test failed for {endpoint_name}", error=str(e))
        
        return {
            "passed": len(bypass_attempts) == 0,
            "bypass_attempts": bypass_attempts,
            "total_endpoints_tested": len(endpoints)
        }
    
    async def _test_rate_limiting(self, base_url: str, endpoints: List[Dict[str, Any]], 
                                 auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test rate limiting functionality."""
        rate_limit_violations = []
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            
            full_url = urljoin(base_url, path)
            auth_headers = await self._prepare_auth_headers(auth_config, full_url, "GET")
            
            # Send rapid requests
            success_count = 0
            for i in range(20):  # Send 20 rapid requests
                try:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        response = await client.get(full_url, headers=auth_headers)
                    
                    if response.status_code == 200:
                        success_count += 1
                    elif response.status_code == 429:  # Rate limited
                        break
                
                except Exception as e:
                    logger.error(f"Rate limiting test failed for {endpoint_name}", error=str(e))
            
            # If all requests succeeded, rate limiting might not be working
            if success_count >= 15:  # Allow some tolerance
                rate_limit_violations.append({
                    "endpoint": endpoint_name,
                    "successful_requests": success_count
                })
        
        return {
            "passed": len(rate_limit_violations) == 0,
            "rate_limit_violations": rate_limit_violations,
            "total_endpoints_tested": len(endpoints)
        }
    
    async def _test_input_validation(self, base_url: str, endpoints: List[Dict[str, Any]], 
                                    auth_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test input validation."""
        validation_failures = []
        
        test_inputs = [
            "very_long_string_" * 1000,  # Very long input
            "<script>alert('test')</script>",  # HTML/JS
            "../../../etc/passwd",  # Path traversal
            "'; DROP TABLE users; --",  # SQL injection
            "test@test.com' OR '1'='1",  # Email with injection
        ]
        
        for endpoint_config in endpoints:
            endpoint_name = endpoint_config.get("name", "Unknown Endpoint")
            path = endpoint_config.get("path", "")
            
            if "?" in path:
                full_url = urljoin(base_url, path)
                auth_headers = await self._prepare_auth_headers(auth_config, full_url, "GET")
                
                for test_input in test_inputs:
                    try:
                        test_url = f"{full_url}&test={test_input}"
                        
                        async with httpx.AsyncClient(timeout=self.timeout) as client:
                            response = await client.get(test_url, headers=auth_headers)
                        
                        # Check if input validation failed (200 response with malicious input)
                        if response.status_code == 200 and test_input in response.text:
                            validation_failures.append({
                                "endpoint": endpoint_name,
                                "test_input": test_input,
                                "status_code": response.status_code
                            })
                    
                    except Exception as e:
                        logger.error(f"Input validation test failed for {endpoint_name}", error=str(e))
        
        return {
            "passed": len(validation_failures) == 0,
            "validation_failures": validation_failures,
            "total_endpoints_tested": len([e for e in endpoints if "?" in e.get("path", "")])
        }
    
    def _calculate_api_test_score(self, test_results: Dict[str, Any]) -> float:
        """Calculate overall API test score."""
        scores = []
        
        # Endpoint test score
        if "endpoint_tests" in test_results:
            endpoint_results = test_results["endpoint_tests"]
            if endpoint_results["total_endpoints"] > 0:
                endpoint_score = (endpoint_results["passed_endpoints"] / endpoint_results["total_endpoints"]) * 100
                scores.append(endpoint_score)
        
        # Contract test score
        if "contract_tests" in test_results:
            contract_results = test_results["contract_tests"]
            if contract_results["total_contracts"] > 0:
                contract_score = (contract_results["passed_contracts"] / contract_results["total_contracts"]) * 100
                scores.append(contract_score)
        
        # Auth test score
        if "auth_tests" in test_results:
            auth_results = test_results["auth_tests"]
            scores.append(auth_results.get("auth_score", 0))
        
        # Load test score
        if "load_tests" in test_results:
            load_results = test_results["load_tests"]
            if load_results["total_scenarios"] > 0:
                load_score = (load_results["passed_scenarios"] / load_results["total_scenarios"]) * 100
                scores.append(load_score)
        
        # Security test score
        if "security_tests" in test_results:
            security_results = test_results["security_tests"]
            scores.append(security_results.get("security_score", 0))
        
        return statistics.mean(scores) if scores else 0.0
    
    def _calculate_auth_score(self, auth_results: Dict[str, Any]) -> float:
        """Calculate authentication test score."""
        scores = []
        
        for auth_type, results in auth_results.items():
            if isinstance(results, dict) and "results" in results:
                total_tests = len(results["results"])
                passed_tests = len([r for r in results["results"].values() if r.get("passed", False)])
                
                if total_tests > 0:
                    score = (passed_tests / total_tests) * 100
                    scores.append(score)
        
        return statistics.mean(scores) if scores else 0.0
    
    def _calculate_security_score(self, security_results: Dict[str, Any]) -> float:
        """Calculate security test score."""
        scores = []
        
        for test_name, result in security_results.items():
            if isinstance(result, dict) and "passed" in result:
                scores.append(100 if result["passed"] else 0)
        
        return statistics.mean(scores) if scores else 0.0
    
    def _calculate_average_response_time(self) -> float:
        """Calculate average response time from test results."""
        response_times = [r.response_time for r in self.test_results if r.response_time > 0]
        return statistics.mean(response_times) if response_times else 0.0
    
    def _calculate_success_rate(self) -> float:
        """Calculate success rate from test results."""
        if not self.test_results:
            return 0.0
        
        successful_tests = len([r for r in self.test_results if r.success])
        return (successful_tests / len(self.test_results)) * 100 