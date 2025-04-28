import os
import sys
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List, Dict, Any
from datetime import datetime
import psutil
import time
import json
from logging.handlers import RotatingFileHandler
import traceback
import threading

import config


class CrawlerLogger:
    """Centralized logging system for the crawler."""
    
    def __init__(self, name: str, log_dir: str = "logs"):
        """
        Initialize the logger.
        
        Args:
            name: Name of the logger
            log_dir: Directory to store log files
        """
        self.name = name
        self.log_dir = log_dir
        self.start_time = time.time()
        self._memory_tracking = False
        self._memory_tracking_lock = threading.Lock()
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Set up logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Create formatters
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler (rotating)
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, f"{name}.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(file_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Performance metrics
        self.metrics: Dict[str, Any] = {
            "start_time": datetime.now().isoformat(),
            "operations": {},
            "memory_usage": [],
            "errors": []
        }
    
    def _get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage in MB."""
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            return {
                "rss": memory_info.rss / (1024 * 1024),  # RSS in MB
                "vms": memory_info.vms / (1024 * 1024),  # VMS in MB
                "percent": process.memory_percent()
            }
        except Exception as e:
            return {
                "rss": 0,
                "vms": 0,
                "percent": 0,
                "error": str(e)
            }
    
    def _log_memory_usage(self):
        """Log current memory usage with recursion guard."""
        with self._memory_tracking_lock:
            if self._memory_tracking:
                return
            
            self._memory_tracking = True
            try:
                memory_usage = self._get_memory_usage()
                self.metrics["memory_usage"].append({
                    "timestamp": datetime.now().isoformat(),
                    "usage": memory_usage
                })
                self.logger.debug(f"Memory usage: {memory_usage}")
            finally:
                self._memory_tracking = False
    
    def _log_operation(self, operation: str, duration: float):
        """Log operation duration."""
        if operation not in self.metrics["operations"]:
            self.metrics["operations"][operation] = []
        self.metrics["operations"][operation].append({
            "timestamp": datetime.now().isoformat(),
            "duration": duration
        })
    
    def _log_error(self, error: Exception, context: Optional[Dict[str, Any]] = None):
        """Log error with context."""
        error_info = {
            "timestamp": datetime.now().isoformat(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "traceback": traceback.format_exc(),
            "context": context or {}
        }
        self.metrics["errors"].append(error_info)
    
    def debug(self, message: str):
        """Log debug message."""
        self._log_memory_usage()
        self.logger.debug(message)
    
    def info(self, message: str):
        """Log info message."""
        self._log_memory_usage()
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message."""
        self._log_memory_usage()
        self.logger.warning(message)
    
    def error(self, message: str, error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None):
        """Log error message with optional exception and context."""
        self._log_memory_usage()
        if error:
            self._log_error(error, context)
        self.logger.error(message, exc_info=error is not None)
    
    def critical(self, message: str, error: Optional[Exception] = None, context: Optional[Dict[str, Any]] = None):
        """Log critical message with optional exception and context."""
        self._log_memory_usage()
        if error:
            self._log_error(error, context)
        self.logger.critical(message, exc_info=error is not None)
    
    def log_operation(self, operation: str):
        """Decorator to log operation duration and memory usage."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration = time.time() - start_time
                    self._log_operation(operation, duration)
                    self.info(f"Operation '{operation}' completed in {duration:.2f}s")
                    return result
                except Exception as e:
                    duration = time.time() - start_time
                    self._log_operation(operation, duration)
                    self.error(f"Operation '{operation}' failed after {duration:.2f}s", e)
                    raise
            return wrapper
        return decorator
    
    def save_metrics(self, filename: Optional[str] = None):
        """Save performance metrics to file."""
        if not filename:
            filename = os.path.join(self.log_dir, f"{self.name}_metrics.json")
        
        with open(filename, 'w') as f:
            json.dump(self.metrics, f, indent=2)
        
        self.info(f"Metrics saved to {filename}")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary of performance metrics."""
        total_duration = time.time() - self.start_time
        
        # Calculate average operation durations
        operation_durations = {}
        for op, durations in self.metrics["operations"].items():
            if durations:
                operation_durations[op] = {
                    "avg": sum(d["duration"] for d in durations) / len(durations),
                    "min": min(d["duration"] for d in durations),
                    "max": max(d["duration"] for d in durations),
                    "count": len(durations)
                }
        
        # Calculate memory usage statistics
        memory_stats = {}
        if self.metrics["memory_usage"]:
            rss_values = [m["usage"]["rss"] for m in self.metrics["memory_usage"]]
            memory_stats = {
                "avg_rss": sum(rss_values) / len(rss_values),
                "max_rss": max(rss_values),
                "min_rss": min(rss_values)
            }
        
        return {
            "total_duration": total_duration,
            "operation_durations": operation_durations,
            "memory_stats": memory_stats,
            "error_count": len(self.metrics["errors"]),
            "start_time": self.metrics["start_time"],
            "end_time": datetime.now().isoformat()
        }


class AlertManager:
    """Alert manager for sending notifications."""
    
    def __init__(self, email: Optional[str] = None):
        """
        Initialize the alert manager.
        
        Args:
            email: Email address to send alerts to (default: from config)
        """
        self.email = email or config.ALERT_EMAIL
        self.logger = CrawlerLogger("alert_manager")
    
    def send_email_alert(self, subject: str, message: str) -> bool:
        """
        Send email alert.
        
        Args:
            subject: Email subject
            message: Email message
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        if not self.email:
            self.logger.warning("Email alert not sent: No email address configured")
            return False
        
        # Create email
        msg = MIMEMultipart()
        msg["From"] = "crawler_alert@example.com"  # Replace with actual sender
        msg["To"] = self.email
        msg["Subject"] = subject
        
        msg.attach(MIMEText(message, "plain"))
        
        try:
            # TODO: Configure SMTP server settings
            # This is just a placeholder
            server = smtplib.SMTP("smtp.example.com", 587)
            server.starttls()
            server.login("username", "password")
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"Email alert sent to {self.email}")
            return True
        
        except Exception as e:
            self.logger.error(f"Failed to send email alert: {str(e)}")
            return False
    
    def log_error(self, source: str, error: str) -> None:
        """
        Log error and send alert if critical.
        
        Args:
            source: Error source
            error: Error message
        """
        self.logger.error(f"[{source}] {error}")
        
        # Send alert for critical errors
        if "critical" in error.lower() or "fatal" in error.lower():
            self.send_email_alert(
                f"CRITICAL ERROR: {source}",
                f"Critical error in {source}:\n\n{error}"
            ) 