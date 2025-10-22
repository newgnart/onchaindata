"""Rate limiting utilities for API clients."""

import time
import logging
import requests
from enum import Enum
from typing import Optional


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    FIXED_INTERVAL = "fixed_interval"
    EXPONENTIAL_BACKOFF = "exponential_backoff"


class RateLimitedSession(requests.Session):
    """Enhanced rate-limited session with multiple strategies."""
    
    def __init__(
        self, 
        calls_per_second: float = 5.0, 
        strategy: RateLimitStrategy = RateLimitStrategy.FIXED_INTERVAL,
        logger: Optional[logging.Logger] = None
    ):
        super().__init__()
        self.calls_per_second = calls_per_second
        self.strategy = strategy
        self.logger = logger or logging.getLogger(__name__)
        self.last_request_time = 0
        self.request_count = 0
        self.min_interval = 1.0 / calls_per_second
        
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a rate-limited request."""
        self._apply_rate_limiting()
        self.request_count += 1
        response = super().request(method, url, **kwargs)
        return response
        
    def _apply_rate_limiting(self):
        """Apply rate limiting based on configured strategy."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if self.strategy == RateLimitStrategy.FIXED_INTERVAL:
            if time_since_last < self.min_interval:
                sleep_time = self.min_interval - time_since_last
                pass  # Rate limiting applied
                time.sleep(sleep_time)
        elif self.strategy == RateLimitStrategy.EXPONENTIAL_BACKOFF:
            # For exponential backoff, we'd need to track consecutive failures
            # This is a simplified implementation
            if time_since_last < self.min_interval:
                backoff_time = min(self.min_interval * (2 ** (self.request_count % 5)), 60)
                pass  # Exponential backoff applied
                time.sleep(backoff_time)
            
        self.last_request_time = time.time()