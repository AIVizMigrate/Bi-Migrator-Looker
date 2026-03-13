"""Tests for Looker DAX API route alignment and env defaults."""

import os
import unittest
from unittest.mock import Mock, patch

from looker_migrator.config.settings import Settings
from looker_migrator.converters.dax_api_client import DaxApiClient, DaxApiConfig


class TestDaxApiUrlNormalization(unittest.TestCase):
    """Ensure configured URLs align with docs/LOOKER_ROUTES.md base route."""

    def test_host_only_url_is_normalized(self):
        client = DaxApiClient(DaxApiConfig(base_url="daxapidemo.azurewebsites.net"))
        self.assertEqual(
            client.config.base_url,
            "https://daxapidemo.azurewebsites.net/looker/convert",
        )

    def test_looker_path_is_normalized(self):
        client = DaxApiClient(DaxApiConfig(base_url="http://localhost:8080/looker"))
        self.assertEqual(client.config.base_url, "http://localhost:8080/looker/convert")

    def test_looker_convert_path_is_preserved(self):
        client = DaxApiClient(
            DaxApiConfig(base_url="https://example.com/looker/convert")
        )
        self.assertEqual(client.config.base_url, "https://example.com/looker/convert")


class TestDaxApiRoutes(unittest.TestCase):
    """Ensure API calls hit expected Looker conversion routes."""

    def test_availability_uses_rag_status_route(self):
        client = DaxApiClient(DaxApiConfig(base_url="daxapidemo.azurewebsites.net"))
        client._session.get = Mock(return_value=Mock(status_code=200))

        available = client.check_availability()

        self.assertTrue(available)
        called_url = client._session.get.call_args.args[0]
        self.assertEqual(
            called_url,
            "https://daxapidemo.azurewebsites.net/looker/convert/rag/status",
        )

    def test_measure_conversion_uses_measure_route(self):
        client = DaxApiClient(DaxApiConfig(base_url="daxapidemo.azurewebsites.net"))
        response = Mock(status_code=200)
        response.json.return_value = {
            "success": True,
            "dax_expression": "SUM(Orders[Revenue])",
            "original_expression": "${TABLE}.revenue",
            "confidence": 0.99,
            "used_llm": True,
            "used_rag": True,
            "warnings": [],
        }
        client._session.post = Mock(return_value=response)

        result = client.convert_measure(
            measure_name="total_revenue",
            measure_type="sum",
            sql_expression="${TABLE}.revenue",
            view_name="orders",
            table_name="Orders",
        )

        self.assertTrue(result.success)
        called_url = client._session.post.call_args.args[0]
        self.assertEqual(
            called_url,
            "https://daxapidemo.azurewebsites.net/looker/convert/measure",
        )


class TestEnvDefaults(unittest.TestCase):
    """Ensure env values feed settings defaults."""

    def test_dax_env_defaults_are_loaded(self):
        with patch.dict(
            os.environ,
            {
                "DAX_API_URL": "daxapidemo.azurewebsites.net",
            },
            clear=True,
        ):
            settings = Settings()

            self.assertEqual(settings.converter.dax_api_url, "daxapidemo.azurewebsites.net")


if __name__ == "__main__":
    unittest.main()
