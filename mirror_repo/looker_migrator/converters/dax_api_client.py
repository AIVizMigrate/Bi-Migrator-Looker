"""
DAX API Client for Looker to Power BI conversion.

Provides a client for the BI-Migrator-DAX-API Looker conversion endpoints.
Enables AI-powered conversion of LookML expressions to DAX with RAG support.
"""

import logging
import requests
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from urllib.parse import urlparse

from ..common.log_utils import log_debug, log_warning, log_info

logger = logging.getLogger(__name__)


@dataclass
class DaxApiConfig:
    """Configuration for DAX API client."""
    base_url: str = "https://daxapidemo.azurewebsites.net"
    timeout: int = 30
    use_rag: bool = True


@dataclass
class DaxApiResponse:
    """Response from DAX API conversion."""
    success: bool
    dax_expression: Optional[str]
    original_expression: Optional[str]
    confidence: float = 0.0
    used_llm: bool = False
    used_rag: bool = False
    error: Optional[str] = None
    warnings: List[str] = None

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


class DaxApiClient:
    """
    Client for the BI-Migrator-DAX-API Looker conversion service.

    Provides methods for converting LookML measures, dimensions, and expressions
    to Power BI DAX using AI-powered conversion with RAG support.
    """

    LOOKER_CONVERT_PREFIX = "/looker/convert"

    def __init__(self, config: Optional[DaxApiConfig] = None):
        """
        Initialize the DAX API client.

        Args:
            config: API configuration
        """
        self.config = config or DaxApiConfig()
        self.config.base_url = self._normalize_base_url(self.config.base_url)
        self._session = requests.Session()
        self._is_available = None
        log_info(f"DAX API configured at: {self.config.base_url}")

    @classmethod
    def _normalize_base_url(cls, base_url: str) -> str:
        """Normalize configured API URL to a Looker convert route root."""
        candidate = (base_url or "").strip()
        if not candidate:
            candidate = DaxApiConfig.base_url

        if not candidate.startswith(("http://", "https://")):
            candidate = f"https://{candidate}"

        candidate = candidate.rstrip("/")
        parsed = urlparse(candidate)
        path = parsed.path.rstrip("/")
        lower_path = path.lower()

        if lower_path.endswith(cls.LOOKER_CONVERT_PREFIX):
            normalized_path = path
        elif lower_path.endswith("/looker"):
            normalized_path = f"{path}/convert"
        elif lower_path.endswith("/convert"):
            normalized_path = path
        else:
            normalized_path = f"{path}{cls.LOOKER_CONVERT_PREFIX}" if path else cls.LOOKER_CONVERT_PREFIX

        return f"{parsed.scheme}://{parsed.netloc}{normalized_path}"

    def _route(self, suffix: str) -> str:
        """Build full route URL under the normalized Looker convert base."""
        return f"{self.config.base_url}/{suffix.lstrip('/')}"

    def check_availability(self) -> bool:
        """
        Check if the DAX API service is available.

        Returns:
            True if service is available
        """
        try:
            response = self._session.get(
                self._route("rag/status"),
                timeout=5
            )
            self._is_available = response.status_code == 200
            return self._is_available
        except Exception as e:
            log_warning(f"DAX API not available: {e}")
            self._is_available = False
            return False

    @property
    def is_available(self) -> bool:
        """Check if the API is available (cached)."""
        if self._is_available is None:
            self.check_availability()
        return self._is_available

    def convert_measure(
        self,
        measure_name: str,
        measure_type: str,
        sql_expression: Optional[str],
        view_name: str,
        table_name: str,
        column_mappings: Optional[Dict[str, str]] = None,
        dependencies: Optional[List[Dict[str, str]]] = None,
        filters: Optional[Dict[str, str]] = None,
    ) -> DaxApiResponse:
        """
        Convert a Looker measure to DAX via the API.

        Args:
            measure_name: Name of the Looker measure
            measure_type: Type of measure (count, sum, etc.)
            sql_expression: SQL/LookML expression
            view_name: Source Looker view name
            table_name: Target Power BI table name
            column_mappings: Optional column mappings
            dependencies: Optional dependent measures
            filters: Optional filters

        Returns:
            DaxApiResponse with conversion result
        """
        try:
            payload = {
                "measure_name": measure_name,
                "measure_type": measure_type,
                "sql_expression": sql_expression,
                "view_name": view_name,
                "table_name": table_name,
                "column_mappings": column_mappings,
                "filters": filters,
                "use_rag": self.config.use_rag,
            }

            if dependencies:
                payload["dependencies"] = [
                    {
                        "name": dep.get("name", ""),
                        "dax": dep.get("dax"),
                        "looker_expression": dep.get("looker_expression"),
                    }
                    for dep in dependencies
                ]

            response = self._session.post(
                self._route("measure"),
                json=payload,
                timeout=self.config.timeout,
            )

            if response.status_code == 200:
                data = response.json()
                return DaxApiResponse(
                    success=data.get("success", False),
                    dax_expression=data.get("dax_expression"),
                    original_expression=data.get("original_expression"),
                    confidence=data.get("confidence", 0.0),
                    used_llm=data.get("used_llm", False),
                    used_rag=data.get("used_rag", False),
                    error=data.get("error"),
                    warnings=data.get("warnings", []),
                )
            else:
                return DaxApiResponse(
                    success=False,
                    dax_expression=None,
                    original_expression=sql_expression,
                    error=f"API error: {response.status_code}",
                )

        except requests.Timeout:
            return DaxApiResponse(
                success=False,
                dax_expression=None,
                original_expression=sql_expression,
                error="API timeout",
            )
        except Exception as e:
            log_warning(f"DAX API call failed: {e}")
            return DaxApiResponse(
                success=False,
                dax_expression=None,
                original_expression=sql_expression,
                error=str(e),
            )

    def convert_dimension(
        self,
        dimension_name: str,
        dimension_type: str,
        sql_expression: Optional[str],
        view_name: str,
        table_name: str,
        column_mappings: Optional[Dict[str, str]] = None,
    ) -> DaxApiResponse:
        """
        Convert a Looker dimension to DAX calculated column via the API.

        Args:
            dimension_name: Name of the Looker dimension
            dimension_type: Type of dimension (string, number, etc.)
            sql_expression: SQL expression
            view_name: Source Looker view name
            table_name: Target Power BI table name
            column_mappings: Optional column mappings

        Returns:
            DaxApiResponse with conversion result
        """
        try:
            payload = {
                "dimension_name": dimension_name,
                "dimension_type": dimension_type,
                "sql_expression": sql_expression,
                "view_name": view_name,
                "table_name": table_name,
                "column_mappings": column_mappings,
                "use_rag": self.config.use_rag,
            }

            response = self._session.post(
                self._route("dimension"),
                json=payload,
                timeout=self.config.timeout,
            )

            if response.status_code == 200:
                data = response.json()
                return DaxApiResponse(
                    success=data.get("success", False),
                    dax_expression=data.get("dax_expression"),
                    original_expression=data.get("original_expression"),
                    confidence=data.get("confidence", 0.0),
                    used_llm=data.get("used_llm", False),
                    used_rag=data.get("used_rag", False),
                    error=data.get("error"),
                    warnings=data.get("warnings", []),
                )
            else:
                return DaxApiResponse(
                    success=False,
                    dax_expression=None,
                    original_expression=sql_expression,
                    error=f"API error: {response.status_code}",
                )

        except requests.Timeout:
            return DaxApiResponse(
                success=False,
                dax_expression=None,
                original_expression=sql_expression,
                error="API timeout",
            )
        except Exception as e:
            log_warning(f"DAX API call failed: {e}")
            return DaxApiResponse(
                success=False,
                dax_expression=None,
                original_expression=sql_expression,
                error=str(e),
            )

    def convert_expression(
        self,
        expression: str,
        expression_type: str,
        view_name: str,
        table_name: str,
        column_mappings: Optional[Dict[str, str]] = None,
        dependencies: Optional[List[Dict[str, str]]] = None,
    ) -> DaxApiResponse:
        """
        Convert a generic Looker expression to DAX via the API.

        Args:
            expression: The Looker expression
            expression_type: Type (measure, dimension, filter)
            view_name: Source Looker view name
            table_name: Target Power BI table name
            column_mappings: Optional column mappings
            dependencies: Optional dependent calculations

        Returns:
            DaxApiResponse with conversion result
        """
        try:
            payload = {
                "expression": expression,
                "expression_type": expression_type,
                "view_name": view_name,
                "table_name": table_name,
                "column_mappings": column_mappings,
                "use_rag": self.config.use_rag,
            }

            if dependencies:
                payload["dependencies"] = [
                    {
                        "name": dep.get("name", ""),
                        "dax": dep.get("dax"),
                        "looker_expression": dep.get("looker_expression"),
                    }
                    for dep in dependencies
                ]

            response = self._session.post(
                self._route("expression"),
                json=payload,
                timeout=self.config.timeout,
            )

            if response.status_code == 200:
                data = response.json()
                return DaxApiResponse(
                    success=data.get("success", False),
                    dax_expression=data.get("dax_expression"),
                    original_expression=data.get("original_expression"),
                    confidence=data.get("confidence", 0.0),
                    used_llm=data.get("used_llm", False),
                    used_rag=data.get("used_rag", False),
                    error=data.get("error"),
                    warnings=data.get("warnings", []),
                )
            else:
                return DaxApiResponse(
                    success=False,
                    dax_expression=None,
                    original_expression=expression,
                    error=f"API error: {response.status_code}",
                )

        except requests.Timeout:
            return DaxApiResponse(
                success=False,
                dax_expression=None,
                original_expression=expression,
                error="API timeout",
            )
        except Exception as e:
            log_warning(f"DAX API call failed: {e}")
            return DaxApiResponse(
                success=False,
                dax_expression=None,
                original_expression=expression,
                error=str(e),
            )

    def convert_batch(
        self,
        items: List[Dict[str, Any]],
        view_name: str,
        table_name: str,
        column_mappings: Optional[Dict[str, str]] = None,
        resolve_dependencies: bool = True,
    ) -> Dict[str, DaxApiResponse]:
        """
        Convert multiple measures/dimensions in a batch.

        Args:
            items: List of items to convert
            view_name: Source Looker view name
            table_name: Target Power BI table name
            column_mappings: Optional column mappings
            resolve_dependencies: Whether to resolve dependencies

        Returns:
            Dictionary mapping item names to DaxApiResponse
        """
        try:
            payload = {
                "items": items,
                "view_name": view_name,
                "table_name": table_name,
                "column_mappings": column_mappings,
                "use_rag": self.config.use_rag,
                "resolve_dependencies": resolve_dependencies,
            }

            response = self._session.post(
                self._route("batch"),
                json=payload,
                timeout=self.config.timeout * 2,  # Double timeout for batch
            )

            results = {}
            if response.status_code == 200:
                data = response.json()
                for item in data.get("items", []):
                    results[item["name"]] = DaxApiResponse(
                        success=item.get("success", False),
                        dax_expression=item.get("dax_expression"),
                        original_expression=item.get("original_expression"),
                        error=item.get("error"),
                    )
            else:
                # Return error for all items
                for item in items:
                    results[item["name"]] = DaxApiResponse(
                        success=False,
                        dax_expression=None,
                        original_expression=item.get("sql_expression"),
                        error=f"API error: {response.status_code}",
                    )

            return results

        except Exception as e:
            log_warning(f"DAX API batch call failed: {e}")
            results = {}
            for item in items:
                results[item["name"]] = DaxApiResponse(
                    success=False,
                    dax_expression=None,
                    original_expression=item.get("sql_expression"),
                    error=str(e),
                )
            return results


# Singleton client instance
_dax_api_client: Optional[DaxApiClient] = None


def get_dax_api_client(config: Optional[DaxApiConfig] = None) -> DaxApiClient:
    """
    Get the singleton DAX API client instance.

    Args:
        config: Optional configuration

    Returns:
        DaxApiClient instance
    """
    global _dax_api_client

    if _dax_api_client is None or config is not None:
        _dax_api_client = DaxApiClient(config)

    return _dax_api_client
