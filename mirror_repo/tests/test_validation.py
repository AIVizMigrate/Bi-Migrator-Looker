"""
Test suite for Looker Migrator validators.

Tests DAX validation, relationship validation, and TMDL validation.
"""

import unittest
import tempfile
from pathlib import Path

from looker_migrator.validators import (
    DAXValidator,
    DAXValidationResult,
    RelationshipValidator,
    RelationshipValidationResult,
    TMDLValidator,
    TMDLValidationResult,
)
from looker_migrator.validators.dax_validator import ValidationSeverity
from looker_migrator.validators.relationship_validator import (
    RelationshipInfo,
    RelationshipSeverity,
)
from looker_migrator.validators.tmdl_validator import TMDLSeverity


class TestDAXValidator(unittest.TestCase):
    """Test DAX expression validation."""

    def setUp(self):
        self.validator = DAXValidator()

    def test_valid_sum_expression(self):
        """Test valid SUM expression."""
        result = self.validator.validate("SUM('Orders'[Amount])")
        self.assertTrue(result.is_valid)
        self.assertFalse(result.has_errors)

    def test_valid_calculate_expression(self):
        """Test valid CALCULATE expression."""
        expr = "CALCULATE(SUM('Sales'[Amount]), 'Products'[Category] = \"Electronics\")"
        result = self.validator.validate(expr)
        self.assertTrue(result.is_valid)

    def test_valid_if_expression(self):
        """Test valid IF expression."""
        expr = "IF('Orders'[Status] = \"Completed\", 1, 0)"
        result = self.validator.validate(expr)
        self.assertTrue(result.is_valid)

    def test_valid_countrows_expression(self):
        """Test valid COUNTROWS expression."""
        result = self.validator.validate("COUNTROWS('Customers')")
        self.assertTrue(result.is_valid)

    def test_empty_expression(self):
        """Test empty expression validation."""
        result = self.validator.validate("")
        self.assertFalse(result.is_valid)
        self.assertTrue(result.has_errors)

    def test_unbalanced_parentheses(self):
        """Test detection of unbalanced parentheses."""
        result = self.validator.validate("SUM('Orders'[Amount]")
        self.assertFalse(result.is_valid)
        self.assertTrue(any("parenthesis" in i.message.lower() for i in result.issues))

    def test_unbalanced_brackets(self):
        """Test detection of unbalanced brackets."""
        result = self.validator.validate("SUM('Orders'[Amount)")
        self.assertFalse(result.is_valid)

    def test_unknown_function_warning(self):
        """Test warning for unknown functions."""
        result = self.validator.validate("UNKNOWNFUNC('Table'[Column])")
        self.assertTrue(result.has_warnings)
        self.assertTrue(any("unknown function" in i.message.lower() for i in result.issues))

    def test_unconverted_looker_reference(self):
        """Test detection of unconverted Looker references."""
        result = self.validator.validate("SUM(${orders.amount})")
        self.assertFalse(result.is_valid)
        self.assertTrue(any("looker" in i.message.lower() for i in result.issues))

    def test_double_equals_warning(self):
        """Test warning for double equals operator."""
        result = self.validator.validate("IF('Status'[Value] == \"Active\", 1, 0)")
        self.assertTrue(result.has_warnings)
        self.assertTrue(any("==" in i.message for i in result.issues))

    def test_null_keyword_warning(self):
        """Test warning for NULL keyword."""
        result = self.validator.validate("IF('Orders'[Amount] = NULL, 0, 'Orders'[Amount])")
        self.assertTrue(result.has_warnings)
        self.assertTrue(any("NULL" in i.message or "BLANK" in i.message for i in result.issues))

    def test_sql_date_format_error(self):
        """Test error for SQL-style date formats."""
        result = self.validator.validate("FORMAT('Date'[OrderDate], \"%Y-%m-%d\")")
        self.assertTrue(result.has_errors)

    def test_strict_mode(self):
        """Test strict validation mode."""
        validator = DAXValidator(strict_mode=True)
        # Expression with warning should fail in strict mode
        result = validator.validate("UNKNOWNFUNC('Table'[Col])")
        self.assertFalse(result.is_valid)

    def test_validate_multiple_expressions(self):
        """Test validating multiple expressions."""
        expressions = [
            "SUM('Orders'[Amount])",
            "COUNTROWS('Customers')",
            "AVERAGE('Products'[Price])",
        ]
        results = self.validator.validate_all(expressions)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(r.is_valid for r in results))


class TestRelationshipValidator(unittest.TestCase):
    """Test relationship validation."""

    def setUp(self):
        self.validator = RelationshipValidator()

    def test_valid_relationship(self):
        """Test valid relationship."""
        relationships = [
            RelationshipInfo(
                name="Orders_Customers",
                from_table="Orders",
                from_column="CustomerID",
                to_table="Customers",
                to_column="CustomerID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertTrue(result.is_valid)

    def test_missing_from_table(self):
        """Test error for missing from_table."""
        relationships = [
            RelationshipInfo(
                name="Invalid",
                from_table="",
                from_column="ID",
                to_table="Customers",
                to_column="ID",
                cardinality="one_to_one",
                cross_filter="single",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("from_table" in i.message.lower() for i in result.issues))

    def test_duplicate_relationship(self):
        """Test detection of duplicate relationships."""
        relationships = [
            RelationshipInfo(
                name="Rel1",
                from_table="Orders",
                from_column="CustomerID",
                to_table="Customers",
                to_column="CustomerID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
            RelationshipInfo(
                name="Rel2",
                from_table="Orders",
                from_column="CustomerID",
                to_table="Customers",
                to_column="CustomerID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertTrue(any("duplicate" in i.message.lower() for i in result.issues))

    def test_invalid_cardinality(self):
        """Test error for invalid cardinality."""
        relationships = [
            RelationshipInfo(
                name="Invalid",
                from_table="A",
                from_column="ID",
                to_table="B",
                to_column="ID",
                cardinality="invalid_cardinality",
                cross_filter="single",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertFalse(result.is_valid)

    def test_bidirectional_warning(self):
        """Test warning for bidirectional cross-filtering."""
        relationships = [
            RelationshipInfo(
                name="BidirectionalRel",
                from_table="Orders",
                from_column="ProductID",
                to_table="Products",
                to_column="ProductID",
                cardinality="many_to_one",
                cross_filter="both",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertTrue(result.has_warnings)
        self.assertTrue(any("bidirectional" in i.message.lower() for i in result.issues))

    def test_self_referencing_info(self):
        """Test info message for self-referencing relationship."""
        relationships = [
            RelationshipInfo(
                name="SelfRef",
                from_table="Employees",
                from_column="ManagerID",
                to_table="Employees",
                to_column="EmployeeID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertTrue(any(
            "self-referencing" in i.message.lower() or "hierarchical" in i.message.lower()
            for i in result.issues
        ))

    def test_many_to_many_warning(self):
        """Test warning for many-to-many relationships."""
        relationships = [
            RelationshipInfo(
                name="M2M",
                from_table="Orders",
                from_column="ProductID",
                to_table="Products",
                to_column="ProductID",
                cardinality="many_to_many",
                cross_filter="single",
            ),
        ]
        result = self.validator.validate(relationships)
        self.assertTrue(any("many-to-many" in i.message.lower() for i in result.issues))

    def test_table_existence_validation(self):
        """Test validation with known tables list."""
        validator = RelationshipValidator(tables=["Orders", "Customers"])
        relationships = [
            RelationshipInfo(
                name="Valid",
                from_table="Orders",
                from_column="CustomerID",
                to_table="Customers",
                to_column="CustomerID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
        ]
        result = validator.validate(relationships)
        self.assertTrue(result.is_valid)

    def test_missing_table_error(self):
        """Test error when table doesn't exist."""
        validator = RelationshipValidator(tables=["Orders"])
        relationships = [
            RelationshipInfo(
                name="Invalid",
                from_table="Orders",
                from_column="ProductID",
                to_table="NonexistentTable",
                to_column="ProductID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
        ]
        result = validator.validate(relationships)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("not found" in i.message.lower() for i in result.issues))

    def test_empty_relationships(self):
        """Test validating empty relationship list."""
        result = self.validator.validate([])
        self.assertTrue(result.is_valid)
        self.assertEqual(result.total_relationships, 0)


class TestTMDLValidator(unittest.TestCase):
    """Test TMDL file validation."""

    def setUp(self):
        self.validator = TMDLValidator()

    def test_validate_valid_model_file(self):
        """Test validating a valid model.tmdl file."""
        content = '''model Model
\tculture: en-US

ref table Orders
ref table Customers
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = Path(tmpdir) / "model.tmdl"
            model_file.write_text(content)

            issues = self.validator.validate_file(model_file)
            errors = [i for i in issues if i.severity == TMDLSeverity.ERROR]
            self.assertEqual(len(errors), 0)

    def test_validate_missing_culture(self):
        """Test warning for missing culture setting."""
        content = '''model Model

ref table Orders
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            model_file = Path(tmpdir) / "model.tmdl"
            model_file.write_text(content)

            issues = self.validator.validate_file(model_file)
            warnings = [i for i in issues if i.severity == TMDLSeverity.WARNING]
            self.assertTrue(any("culture" in i.message.lower() for i in warnings))

    def test_validate_table_file(self):
        """Test validating a table.tmdl file."""
        content = '''/// Table: Orders
table 'Orders'
\tlineageTag: orders-table

\tcolumn 'OrderID'
\t\tdataType: int64
\t\tlineageTag: order-id
\t\tsummarizeBy: none

\tmeasure 'Total Sales'
\t\tlineageTag: total-sales
\t\texpression: ```
\t\t\tSUM('Orders'[Amount])
\t\t\t```
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            table_file = Path(tmpdir) / "Orders.tmdl"
            table_file.write_text(content)

            issues = self.validator.validate_file(table_file)
            errors = [i for i in issues if i.severity == TMDLSeverity.ERROR]
            self.assertEqual(len(errors), 0)

    def test_validate_missing_expression(self):
        """Test error for measure missing expression."""
        content = '''table 'Orders'
\tlineageTag: orders

\tmeasure 'Broken Measure'
\t\tlineageTag: broken
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            table_file = Path(tmpdir) / "Orders.tmdl"
            table_file.write_text(content)

            issues = self.validator.validate_file(table_file)
            errors = [i for i in issues if i.severity == TMDLSeverity.ERROR]
            self.assertTrue(any("expression" in i.message.lower() for i in errors))

    def test_validate_unconverted_looker_syntax(self):
        """Test error for unconverted Looker syntax in TMDL."""
        content = '''table 'Orders'
\tlineageTag: orders

\tmeasure 'Total'
\t\tlineageTag: total
\t\texpression: ```
\t\t\tSUM(${orders.amount})
\t\t\t```
'''
        with tempfile.TemporaryDirectory() as tmpdir:
            table_file = Path(tmpdir) / "Orders.tmdl"
            table_file.write_text(content)

            issues = self.validator.validate_file(table_file)
            errors = [i for i in issues if i.severity == TMDLSeverity.ERROR]
            self.assertTrue(any("looker" in i.message.lower() for i in errors))

    def test_validate_directory_missing_files(self):
        """Test validation of directory missing required files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create empty directory - no required files
            result = self.validator.validate_directory(tmpdir)
            self.assertFalse(result.is_valid)
            self.assertTrue(any("required" in i.message.lower() or "no .tmdl" in i.message.lower()
                              for i in result.issues))

    def test_validate_complete_directory(self):
        """Test validation of complete TMDL directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Create required structure
            (tmppath / "definition").mkdir()

            # model.tmdl
            (tmppath / "model.tmdl").write_text('''model TestModel
\tculture: en-US
''')

            # database.tmdl
            (tmppath / "definition" / "database.tmdl").write_text('''database Database
\tcompatibilityLevel: 1567
''')

            result = self.validator.validate_directory(tmpdir)
            # Should not have errors for required files
            file_errors = [i for i in result.issues
                          if i.severity == TMDLSeverity.ERROR and "required" in i.message.lower()]
            self.assertEqual(len(file_errors), 0)

    def test_validate_nonexistent_directory(self):
        """Test validation of nonexistent directory."""
        result = self.validator.validate_directory("/nonexistent/path")
        self.assertFalse(result.is_valid)
        self.assertTrue(any("not found" in i.message.lower() for i in result.issues))

    def test_validate_empty_file(self):
        """Test validation of empty TMDL file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_file = Path(tmpdir) / "empty.tmdl"
            empty_file.write_text("")

            issues = self.validator.validate_file(empty_file)
            warnings = [i for i in issues if i.severity == TMDLSeverity.WARNING]
            self.assertTrue(any("empty" in i.message.lower() for i in warnings))


class TestValidatorIntegration(unittest.TestCase):
    """Integration tests for validators working together."""

    def test_full_validation_pipeline(self):
        """Test running all validators on generated output."""
        # This would be a more complete integration test
        # that validates actual migration output
        dax_validator = DAXValidator()
        rel_validator = RelationshipValidator()
        tmdl_validator = TMDLValidator()

        # Validate some expressions
        dax_results = dax_validator.validate_all([
            "SUM('Orders'[Amount])",
            "COUNTROWS('Customers')",
            "CALCULATE(SUM('Sales'[Revenue]), 'Date'[Year] = 2024)",
        ])
        self.assertTrue(all(r.is_valid for r in dax_results))

        # Validate relationships
        rel_result = rel_validator.validate([
            RelationshipInfo(
                name="Orders_Customers",
                from_table="Orders",
                from_column="CustomerID",
                to_table="Customers",
                to_column="CustomerID",
                cardinality="many_to_one",
                cross_filter="single",
            ),
        ])
        self.assertTrue(rel_result.is_valid)


if __name__ == "__main__":
    unittest.main()
