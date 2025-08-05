"""
Configuration Manager

This module provides configuration management capabilities for loading and managing
configuration files, environment variables, and settings.
"""

import os
import yaml
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union
import structlog

logger = structlog.get_logger()


class ConfigManager:
    """
    Configuration manager for handling project configuration.
    
    This class provides functionality to load and manage configuration from
    multiple sources including YAML files, JSON files, and environment variables.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config_data: Dict[str, Any] = {}
        
        # Load configuration
        self._load_configuration()
        
        logger.info("Configuration manager initialized", config_path=config_path)
    
    def _load_configuration(self) -> None:
        """Load configuration from multiple sources."""
        # Load from specified config file
        if self.config_path and Path(self.config_path).exists():
            self._load_from_file(self.config_path)
        
        # Load from default config files
        default_config_paths = [
            "config/config.yaml",
            "config/config.yml",
            "config/config.json",
            ".env"
        ]
        
        for config_path in default_config_paths:
            if Path(config_path).exists():
                self._load_from_file(config_path)
        
        # Load from environment variables
        self._load_from_environment()
        
        logger.info("Configuration loaded", config_keys=list(self.config_data.keys()))
    
    def _load_from_file(self, file_path: str) -> None:
        """Load configuration from a file."""
        try:
            file_path = Path(file_path)
            
            if file_path.suffix.lower() in ['.yaml', '.yml']:
                with open(file_path, 'r', encoding='utf-8') as file:
                    file_config = yaml.safe_load(file)
                    if file_config:
                        self.config_data.update(file_config)
            
            elif file_path.suffix.lower() == '.json':
                with open(file_path, 'r', encoding='utf-8') as file:
                    file_config = json.load(file)
                    if file_config:
                        self.config_data.update(file_config)
            
            elif file_path.name == '.env':
                self._load_env_file(file_path)
            
            logger.info(f"Configuration loaded from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load configuration from {file_path}", error=str(e))
    
    def _load_env_file(self, file_path: Path) -> None:
        """Load configuration from .env file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        self.config_data[key.strip()] = value.strip()
        except Exception as e:
            logger.error(f"Failed to load .env file {file_path}", error=str(e))
    
    def _load_from_environment(self) -> None:
        """Load configuration from environment variables."""
        # Common environment variables for data engineering
        env_mappings = {
            'DATABASE_URL': 'database.url',
            'DB_HOST': 'database.host',
            'DB_PORT': 'database.port',
            'DB_NAME': 'database.name',
            'DB_USER': 'database.user',
            'DB_PASSWORD': 'database.password',
            'API_BASE_URL': 'api.base_url',
            'API_KEY': 'api.key',
            'API_SECRET': 'api.secret',
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka.bootstrap_servers',
            'REDIS_URL': 'redis.url',
            'LOG_LEVEL': 'logging.level',
            'ENVIRONMENT': 'environment'
        }
        
        for env_var, config_key in env_mappings.items():
            env_value = os.getenv(env_var)
            if env_value:
                self._set_nested_config(config_key, env_value)
    
    def _set_nested_config(self, key_path: str, value: Any) -> None:
        """Set a nested configuration value using dot notation."""
        keys = key_path.split('.')
        current = self.config_data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def get_config(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation)
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        try:
            keys = key.split('.')
            current = self.config_data
            
            for k in keys:
                if isinstance(current, dict) and k in current:
                    current = current[k]
                else:
                    return default
            
            return current
            
        except Exception as e:
            logger.error(f"Failed to get config for key {key}", error=str(e))
            return default
    
    def set_config(self, key: str, value: Any) -> None:
        """
        Set configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation)
            value: Configuration value
        """
        try:
            self._set_nested_config(key, value)
            logger.info(f"Configuration set: {key} = {value}")
        except Exception as e:
            logger.error(f"Failed to set config for key {key}", error=str(e))
    
    def get_database_config(self, database_name: str = "default") -> Dict[str, Any]:
        """
        Get database configuration.
        
        Args:
            database_name: Name of the database configuration
            
        Returns:
            Database configuration dictionary
        """
        db_config = self.get_config(f"databases.{database_name}", {})
        
        # Fallback to default database config
        if not db_config:
            db_config = self.get_config("database", {})
        
        return db_config
    
    def get_api_config(self, api_name: str = "default") -> Dict[str, Any]:
        """
        Get API configuration.
        
        Args:
            api_name: Name of the API configuration
            
        Returns:
            API configuration dictionary
        """
        api_config = self.get_config(f"apis.{api_name}", {})
        
        # Fallback to default API config
        if not api_config:
            api_config = self.get_config("api", {})
        
        return api_config
    
    def get_quality_thresholds(self) -> Dict[str, Any]:
        """
        Get data quality thresholds.
        
        Returns:
            Quality thresholds dictionary
        """
        return self.get_config("quality_thresholds", {
            "completeness_threshold": 95,
            "accuracy_threshold": 95,
            "anomaly_threshold": 5,
            "consistency_threshold": 90
        })
    
    def get_performance_thresholds(self) -> Dict[str, Any]:
        """
        Get performance thresholds.
        
        Returns:
            Performance thresholds dictionary
        """
        return self.get_config("performance_thresholds", {
            "query_performance_threshold": 80,
            "load_test_threshold": 85,
            "stress_test_threshold": 70,
            "benchmark_threshold": 90
        })
    
    def get_api_thresholds(self) -> Dict[str, Any]:
        """
        Get API testing thresholds.
        
        Returns:
            API thresholds dictionary
        """
        return self.get_config("api_thresholds", {
            "api_test_threshold": 85,
            "response_time_threshold": 2.0,
            "error_rate_threshold": 5.0
        })
    
    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration.
        
        Returns:
            Logging configuration dictionary
        """
        return self.get_config("logging", {
            "level": "INFO",
            "format": "json",
            "file": "logs/qa_engineer.log"
        })
    
    def get_environment(self) -> str:
        """
        Get current environment.
        
        Returns:
            Environment name
        """
        return self.get_config("environment", "development")
    
    def is_production(self) -> bool:
        """
        Check if running in production environment.
        
        Returns:
            True if production environment
        """
        return self.get_environment().lower() == "production"
    
    def is_development(self) -> bool:
        """
        Check if running in development environment.
        
        Returns:
            True if development environment
        """
        return self.get_environment().lower() == "development"
    
    def is_testing(self) -> bool:
        """
        Check if running in testing environment.
        
        Returns:
            True if testing environment
        """
        return self.get_environment().lower() == "testing"
    
    def get_all_config(self) -> Dict[str, Any]:
        """
        Get all configuration data.
        
        Returns:
            Complete configuration dictionary
        """
        return self.config_data.copy()
    
    def reload_config(self) -> None:
        """Reload configuration from all sources."""
        self.config_data.clear()
        self._load_configuration()
        logger.info("Configuration reloaded")
    
    def validate_config(self) -> Dict[str, Any]:
        """
        Validate configuration completeness.
        
        Returns:
            Validation results
        """
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Check required configurations
        required_configs = [
            "database",
            "api",
            "quality_thresholds",
            "performance_thresholds"
        ]
        
        for config_key in required_configs:
            if not self.get_config(config_key):
                validation_results["errors"].append(f"Missing required configuration: {config_key}")
                validation_results["valid"] = False
        
        # Check database configuration
        db_config = self.get_database_config()
        if db_config:
            required_db_fields = ["host", "port", "name", "user"]
            for field in required_db_fields:
                if not db_config.get(field):
                    validation_results["warnings"].append(f"Missing database field: {field}")
        
        # Check API configuration
        api_config = self.get_api_config()
        if api_config:
            if not api_config.get("base_url"):
                validation_results["warnings"].append("Missing API base URL")
        
        logger.info("Configuration validation completed", 
                   valid=validation_results["valid"],
                   error_count=len(validation_results["errors"]),
                   warning_count=len(validation_results["warnings"]))
        
        return validation_results 