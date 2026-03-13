"""
Calculation Tracker for Looker to Power BI migration.

Tracks all calculations (measures and calculated dimensions) through the migration process,
including their conversion status, method (AI vs rule-based), and metadata.

Matches the Tableau migrator pattern exactly:
  1. add_looker_calculation()   — register with FormulaDax='' and Status='extracted'
  2. _save_calculations()       — persist to extracted/calculations.json
  3. update_powerbi_calculation() — fill in DAX after conversion
  4. _save_calculations()       — persist updated calculations.json
"""

import json
import os
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from enum import Enum
from pathlib import Path

from .websocket_client import send_conversion_progress

logger = logging.getLogger('looker_migrator')


class CalculationStatus(Enum):
    """Status of a calculation in the migration process."""
    EXTRACTED = "extracted"
    CONVERTING = "converting"
    CONVERTED = "converted"
    FAILED = "failed"
    SKIPPED = "skipped"


class ConversionMethod(Enum):
    """Method used for conversion."""
    AI = "AI"
    RULE_BASED = "rule-based"
    MANUAL_REQUIRED = "manual-required"


class CalculationTracker:
    """
    Tracks all calculations through the migration process.

    Matches Tableau's CalculationTracker pattern exactly:
    - calculations dict keyed by "{table_name}_{calculation_name}"
    - add_looker_calculation() registers before conversion (FormulaDax='')
    - update_powerbi_calculation() updates after conversion
    - _save_calculations() persists atomically to disk after each operation
    """

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        task_id: Optional[str] = None,
    ):
        """
        Initialize the calculation tracker.

        Args:
            output_dir: Base output directory (calculations.json written to extracted/)
            task_id: Task identifier for WebSocket progress reporting
        """
        self.task_id = task_id
        self.output_dir = Path(output_dir) if output_dir else None
        self.extracted_dir: Optional[Path] = None
        self._json_path: Optional[Path] = None

        if self.output_dir:
            self.extracted_dir = self.output_dir / "extracted"
            self.extracted_dir.mkdir(parents=True, exist_ok=True)
            self._json_path = self.extracted_dir / "calculations.json"

        # Dict keyed by "{table_name}_{name}" — matches Tableau's pattern
        self.calculations: Dict[str, Dict[str, Any]] = {}
        self._calculation_index = 0
        self._total_calculations = 0

        # Statistics
        self.stats = {
            "total": 0,
            "converted": 0,
            "failed": 0,
            "skipped": 0,
            "ai_converted": 0,
            "rule_based": 0,
        }

        # Load existing calculations if file exists (matches Tableau)
        if self._json_path and self._json_path.exists():
            try:
                with open(self._json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if "calculations" in data:
                    for calc in data["calculations"]:
                        key = f"{calc.get('TableName', '')}_{calc.get('CalculationName', '')}"
                        self.calculations[key] = calc
            except Exception as e:
                logger.warning(f"Could not load existing calculations.json: {e}")
        else:
            self._save_calculations()

    def set_task_id(self, task_id: str) -> None:
        """Set the task ID for progress reporting."""
        self.task_id = task_id

    def set_output_dir(self, output_dir: Path) -> None:
        """Set or update the output directory for calculations.json."""
        self.output_dir = Path(output_dir)
        self.extracted_dir = self.output_dir / "extracted"
        self.extracted_dir.mkdir(parents=True, exist_ok=True)
        self._json_path = self.extracted_dir / "calculations.json"

    def set_total_calculations(self, total: int) -> None:
        """
        Set the total number of calculations upfront.

        Called BEFORE conversion starts so progress shows "X/TOTAL" correctly
        (matching Tableau's pattern where total is known before conversion begins).

        Args:
            total: Total number of calculations across all views
        """
        self._total_calculations = total

    def add_looker_calculation(
        self,
        table_name: str,
        calculation_name: str,
        expression: str,
        formula_type: str,
        looker_type: Optional[str] = None,
        description: Optional[str] = None,
        data_type: Optional[str] = None,
    ) -> None:
        """
        Register a calculation BEFORE conversion (matches Tableau's add_tableau_calculation).

        Saves with FormulaDax='' and Status='extracted'. The DAX formula is filled in
        later by update_powerbi_calculation().

        Args:
            table_name: Power BI table name
            calculation_name: Calculation name (used as identifier)
            expression: Original Looker formula/SQL
            formula_type: 'measure' or 'calculated_column'
            looker_type: Looker type (count, sum, number, etc.)
            description: Optional description
            data_type: Optional data type
        """
        key = f"{table_name}_{calculation_name}"
        if key in self.calculations:
            return

        self.calculations[key] = {
            "TableName": table_name,
            "CalculationName": calculation_name,
            "FormulaLooker": expression,
            "FormulaTypeLooker": formula_type,
            "LookerType": looker_type,
            "PowerBIName": calculation_name,
            "FormulaDax": "",
            "Status": CalculationStatus.EXTRACTED.value,
            "ConversionMethod": None,
            "Confidence": 0.0,
            "UsedApi": False,
            "DataType": data_type or "string",
            "Description": description,
            "Warnings": [],
        }

        self.stats["total"] += 1
        if self._total_calculations < self.stats["total"]:
            self._total_calculations = self.stats["total"]

        self._save_calculations()

    def update_powerbi_calculation(
        self,
        table_name: str,
        calculation_name: str,
        powerbi_name: str,
        dax_expression: str,
        conversion_method: str = "rule-based",
        confidence: float = 1.0,
        used_api: bool = False,
        format_string: Optional[str] = None,
        summarize_by: str = "none",
        warnings: Optional[List[str]] = None,
    ) -> None:
        """
        Update a calculation AFTER conversion (matches Tableau's update_powerbi_calculation).

        Args:
            table_name: Table name
            calculation_name: Calculation name (identifier from add_looker_calculation)
            powerbi_name: Name in Power BI
            dax_expression: Converted DAX expression
            conversion_method: "AI" or "rule-based"
            confidence: Conversion confidence score
            used_api: Whether AI API was used
            format_string: Power BI format string
            summarize_by: Summarization type
            warnings: Any conversion warnings
        """
        key = f"{table_name}_{calculation_name}"
        calc = self.calculations.get(key)
        if not calc:
            return

        # Determine status based on DAX expression (matches Tableau pattern)
        is_error = dax_expression.startswith("/* ERROR") if dax_expression else True
        status = CalculationStatus.FAILED.value if is_error else CalculationStatus.CONVERTED.value

        calc["PowerBIName"] = powerbi_name
        calc["FormulaDax"] = dax_expression
        calc["Status"] = status
        calc["ConversionMethod"] = conversion_method
        calc["Confidence"] = confidence
        calc["UsedApi"] = used_api
        calc["SummarizeBy"] = summarize_by
        if format_string:
            calc["FormatString"] = format_string
        if warnings:
            calc.setdefault("Warnings", []).extend(warnings)

        # Update stats
        if is_error:
            self.stats["failed"] += 1
        else:
            self.stats["converted"] += 1
            if used_api or conversion_method == ConversionMethod.AI.value:
                self.stats["ai_converted"] += 1
            else:
                self.stats["rule_based"] += 1

        # Send progress via WebSocket
        self._calculation_index += 1
        self._send_progress(calc, is_error=is_error)

        self._save_calculations()

    def fail_conversion(
        self,
        name: str,
        table_name: str,
        error: Optional[str] = None,
    ) -> None:
        """
        Mark a calculation as failed.

        Args:
            name: Calculation name
            table_name: Table name
            error: Error message
        """
        key = f"{table_name}_{name}"
        calc = self.calculations.get(key)
        if not calc:
            return

        calc["Status"] = CalculationStatus.FAILED.value
        calc["ConversionMethod"] = ConversionMethod.MANUAL_REQUIRED.value
        if error:
            calc.setdefault("Warnings", []).append(f"Conversion failed: {error}")

        self.stats["failed"] += 1
        self._calculation_index += 1
        self._send_progress(calc, is_error=True)
        self._save_calculations()

    def skip_calculation(
        self,
        name: str,
        table_name: str,
        reason: Optional[str] = None,
    ) -> None:
        """
        Mark a calculation as skipped.

        Args:
            name: Calculation name
            table_name: Table name
            reason: Reason for skipping
        """
        key = f"{table_name}_{name}"
        calc = self.calculations.get(key)
        if not calc:
            return

        calc["Status"] = CalculationStatus.SKIPPED.value
        if reason:
            calc.setdefault("Warnings", []).append(f"Skipped: {reason}")

        self.stats["skipped"] += 1
        self._calculation_index += 1
        self._save_calculations()

    def _save_calculations(self) -> None:
        """
        Persist calculations to disk atomically (matches Tableau's _save_calculations).

        Writes to a temp file then replaces for atomic durability.
        """
        if not self._json_path:
            return

        try:
            self._json_path.parent.mkdir(parents=True, exist_ok=True)

            calculations_data = {
                "summary": self.get_summary(),
                "calculations": list(self.calculations.values()),
            }

            tmp_path = self._json_path.with_suffix(".json.tmp")
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(calculations_data, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())

            tmp_path.replace(self._json_path)
        except Exception as e:
            logger.error(f"Failed to save calculations.json: {e}")

    def _send_progress(self, calc: Dict[str, Any], is_error: bool = False) -> None:
        """Send progress update via WebSocket."""
        if not self.task_id:
            return

        send_conversion_progress(
            task_id=self.task_id,
            calculation_name=calc.get("CalculationName", ""),
            calculation_index=self._calculation_index,
            total_calculations=self._total_calculations,
            conversion_method=calc.get("ConversionMethod") or "rule-based",
            table_name=calc.get("TableName", ""),
            calculation_type=calc.get("FormulaTypeLooker", "measure"),
            status=calc.get("Status", ""),
            confidence=calc.get("Confidence", 0.0),
        )

    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        return {
            "total_calculations": self.stats["total"],
            "converted": self.stats["converted"],
            "failed": self.stats["failed"],
            "skipped": self.stats["skipped"],
            "ai_converted": self.stats["ai_converted"],
            "rule_based": self.stats["rule_based"],
            "conversion_rate": (
                self.stats["converted"] / self.stats["total"] * 100
                if self.stats["total"] > 0 else 0
            ),
        }

    def export_calculations_json(self, output_path: Path) -> Path:
        """
        Export calculations to JSON file.

        If output_dir was set, this just returns the existing path.
        Otherwise writes to the given output_path.

        Args:
            output_path: Directory to write calculations.json

        Returns:
            Path to the generated file
        """
        # If we already have a json path and it's up-to-date, just ensure it's saved
        if self._json_path and self._json_path.parent == output_path:
            self._save_calculations()
            return self._json_path

        # Write to the requested path
        output_path.mkdir(parents=True, exist_ok=True)
        output_file = output_path / "calculations.json"

        calculations_data = {
            "summary": self.get_summary(),
            "calculations": list(self.calculations.values()),
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(calculations_data, f, indent=2, default=str)

        return output_file

    def get_calculations_for_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all calculations for a specific table."""
        return [
            c for c in self.calculations.values()
            if c.get("TableName") == table_name
        ]

    def get_measures(self) -> List[Dict[str, Any]]:
        """Get all measure calculations."""
        return [
            c for c in self.calculations.values()
            if c.get("FormulaTypeLooker") == "measure"
        ]

    def get_dimensions(self) -> List[Dict[str, Any]]:
        """Get all calculated dimension calculations."""
        return [
            c for c in self.calculations.values()
            if c.get("FormulaTypeLooker") == "calculated_column"
        ]


# Module-level singleton for easy access
_tracker_instance: Optional[CalculationTracker] = None


def get_calculation_tracker(
    task_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> CalculationTracker:
    """
    Get or create the calculation tracker singleton.

    Args:
        task_id: Optional task ID to set
        output_dir: Optional output directory for calculations.json

    Returns:
        The calculation tracker instance
    """
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = CalculationTracker(output_dir=output_dir, task_id=task_id)
    else:
        if task_id:
            _tracker_instance.set_task_id(task_id)
        if output_dir:
            _tracker_instance.set_output_dir(output_dir)
    return _tracker_instance


def reset_calculation_tracker() -> None:
    """Reset the calculation tracker singleton."""
    global _tracker_instance
    _tracker_instance = None
