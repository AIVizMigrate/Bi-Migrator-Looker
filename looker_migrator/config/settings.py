"""
Settings module for Looker Migrator.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import yaml


def _default_dax_api_url() -> str:
    """Resolve default DAX API URL from env."""
    return (
        os.getenv("LOOKER_DAX_API_URL")
        or os.getenv("DAX_API_URL")
        or "https://daxapidemo.azurewebsites.net"
    )


@dataclass
class ParserSettings:
    """Parser configuration."""
    max_file_size_mb: int = 100
    default_encoding: str = "utf-8"
    resolve_extends: bool = True
    skip_invalid_views: bool = True


@dataclass
class ConverterSettings:
    """Converter configuration."""
    default_connection_type: str = "sql_server"
    convert_derived_tables: bool = True
    dax_api_url: str = field(default_factory=_default_dax_api_url)  # DAX API endpoint (host/path supported)
    dax_api_timeout: int = 30  # API timeout in seconds
    use_rag: bool = True  # Use RAG for enhanced conversion


@dataclass
class GeneratorSettings:
    """Generator configuration."""
    tmdl_version: str = "1567"
    culture: str = "en-US"
    sanitize_names: bool = True


@dataclass
class OutputSettings:
    """Output configuration."""
    output_encoding: str = "utf-8"
    log_level: str = "INFO"


@dataclass
class Settings:
    """Main settings container."""
    parser: ParserSettings = field(default_factory=ParserSettings)
    converter: ConverterSettings = field(default_factory=ConverterSettings)
    generator: GeneratorSettings = field(default_factory=GeneratorSettings)
    output: OutputSettings = field(default_factory=OutputSettings)

    job_id: Optional[str] = None
    verbose: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> "Settings":
        """Create Settings from dictionary."""
        settings = cls()

        if "parser" in data:
            for key, value in data["parser"].items():
                if hasattr(settings.parser, key):
                    setattr(settings.parser, key, value)

        if "converter" in data:
            for key, value in data["converter"].items():
                if hasattr(settings.converter, key):
                    setattr(settings.converter, key, value)

        if "generator" in data:
            for key, value in data["generator"].items():
                if hasattr(settings.generator, key):
                    setattr(settings.generator, key, value)

        if "output" in data:
            for key, value in data["output"].items():
                if hasattr(settings.output, key):
                    setattr(settings.output, key, value)

        if "job_id" in data:
            settings.job_id = data["job_id"]
        if "verbose" in data:
            settings.verbose = data["verbose"]

        return settings

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "Settings":
        """Load settings from YAML file."""
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
        return cls.from_dict(data)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "parser": {
                "max_file_size_mb": self.parser.max_file_size_mb,
                "default_encoding": self.parser.default_encoding,
                "resolve_extends": self.parser.resolve_extends,
                "skip_invalid_views": self.parser.skip_invalid_views,
            },
            "converter": {
                "default_connection_type": self.converter.default_connection_type,
                "convert_derived_tables": self.converter.convert_derived_tables,
                "dax_api_url": self.converter.dax_api_url,
                "dax_api_timeout": self.converter.dax_api_timeout,
                "use_rag": self.converter.use_rag,
            },
            "generator": {
                "tmdl_version": self.generator.tmdl_version,
                "culture": self.generator.culture,
                "sanitize_names": self.generator.sanitize_names,
            },
            "output": {
                "output_encoding": self.output.output_encoding,
                "log_level": self.output.log_level,
            },
            "job_id": self.job_id,
            "verbose": self.verbose,
        }


def load_settings(
    yaml_path: Optional[str] = None,
    overrides: Optional[dict] = None,
) -> Settings:
    """Load settings from file and/or overrides."""
    settings = Settings()

    if yaml_path and Path(yaml_path).exists():
        settings = Settings.from_yaml(yaml_path)

    if overrides:
        override_settings = Settings.from_dict(overrides)
        # Simple merge
        settings = override_settings

    return settings
