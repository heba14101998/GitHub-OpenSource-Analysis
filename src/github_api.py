"""
This module is a base class for interacting with the GitHub API.

    - It provides methods for making requests to the GitHub API
    - Use rate limiting, retry logic, and error handling.
    - It utilizes a provided GitHub token for authorization and logging to track API activity.
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, Optional
from dotenv import load_dotenv

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

class GitHubAPI:
    """Base class for interacting with the GitHub API."""

    def __init__(self):

        # Load token from environment variable
        load_dotenv('.env')
        token = os.getenv("YOUR_GITHUB_TOKEN")

        if not token:
            raise ValueError("GitHub token not provided")

        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"token {token}",
        }

        # Define log file name and path
        self.log_file_path = os.path.join(
            os.getcwd(), 'logs',
            f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
        )
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Sets up the logger for the GitHub API."""

        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)

        # Configure basic logging setup
        logging.basicConfig(
            filename=self.log_file_path,
            format='[%(asctime)s] - %(levelname)s - %(lineno)d - %(message)s',
            level=logging.INFO,
        )

        return logging.getLogger(__name__)

    def _create_retry_session(self) -> requests.Session:
        """Creates a requests session with retry capabilities."""

        retries = Retry(
            total=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=0.2,  # Wait exponentially longer
            respect_retry_after_header=True,
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        return http

    def _handle_api_errors(
            self,
            response: requests.Response) -> Optional[Dict]:
        """Handles common API errors, returns data if successful."""

        if response.status_code == 200:
            return response.json()

        elif response.status_code == 403:
            wait_time = (
                int(response.headers["X-RateLimit-Reset"]) - time.time()) / 60
            self.logger.warning(
                "Rate limit exceeded! Please wait and try again after :%s minutes.",
                int(wait_time))
            return None

        elif response.status_code == 404:
            self.logger.error("Error 404: Resource not found.")
            return None

        elif response.status_code == 451:
            self.logger.error(
                "Error %s Unavailable For Legal Reasons.",
                response.status_code
            )
            return None

        elif response.status_code == 401:
            self.logger.error("Unauthorized (401) - Check your GitHub token.")
            return None

        elif response.status_code == 204:
            self.logger.info(
                "HTTP 204 - No Content. This could be expected, but investigate."
            )
            return None

        elif response.status_code == 409:
            self.logger.error(
                "HTTP 409 - Conflict. This often means a resource exists but "
                "should not or already has an operation running on it."
            )
            return None

        else:
            self.logger.error("Error fetching data: %s", response.status_code)
            return None

    def _get(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """Makes a GET request to the GitHub API."""

        http = self._create_retry_session()

        response = http.get(url, headers=self.headers, params=params)
        
        return self._handle_api_errors(response)
