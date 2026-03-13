"""
Test suite for Looker to Power BI migration.

Tests the complete migration pipeline from LookML to TMDL.
"""

import unittest
import tempfile
import os
from pathlib import Path
from typing import Optional

# Import migrator components
from looker_migrator import (
    LookerMigrator,
    migrate_lookml_project,
    migrate_lookml_view,
    Settings,
)
from looker_migrator.models import (
    LookmlProject,
    LookmlModel,
    LookmlView,
    LookmlExplore,
    LookmlDimension,
    LookmlMeasure,
    LookmlJoin,
    PbiModel,
    PbiTable,
    PbiColumn,
    PbiMeasure,
    PbiRelationship,
    Cardinality,
    CrossFilterDirection,
    LookerDataType,
    PbiDataType,
)
from looker_migrator.parsers import LookmlParser, ProjectParser
from looker_migrator.converters import (
    ExpressionConverter,
    SqlToDaxConverter,
    DatatypeMapper,
    JoinConverter,
)
from looker_migrator.generators import ModelGenerator, TmdlGenerator


class TestLookmlModels(unittest.TestCase):
    """Test LookML data model classes."""

    def test_lookml_dimension_creation(self):
        """Test creating a LookML dimension."""
        dim = LookmlDimension(
            name="customer_id",
            type=LookerDataType.STRING,
            sql="${TABLE}.customer_id",
            description="Customer identifier",
        )
        self.assertEqual(dim.name, "customer_id")
        self.assertEqual(dim.type, LookerDataType.STRING)
        self.assertIn("customer_id", dim.sql)

    def test_lookml_measure_creation(self):
        """Test creating a LookML measure."""
        measure = LookmlMeasure(
            name="total_sales",
            type="sum",
            sql="${TABLE}.sales_amount",
            description="Total sales amount",
        )
        self.assertEqual(measure.name, "total_sales")
        self.assertEqual(measure.type, "sum")

    def test_lookml_view_creation(self):
        """Test creating a LookML view with dimensions and measures."""
        view = LookmlView(
            name="orders",
            sql_table_name="public.orders",
            dimensions=[
                LookmlDimension(name="order_id", type=LookerDataType.NUMBER),
                LookmlDimension(name="order_date", type=LookerDataType.DATE),
            ],
            measures=[
                LookmlMeasure(name="order_count", type="count"),
            ],
        )
        self.assertEqual(view.name, "orders")
        self.assertEqual(len(view.dimensions), 2)
        self.assertEqual(len(view.measures), 1)

    def test_lookml_explore_creation(self):
        """Test creating a LookML explore with joins."""
        explore = LookmlExplore(
            name="order_items",
            from_view="orders",
            joins=[
                LookmlJoin(
                    name="products",
                    sql_on="${orders.product_id} = ${products.id}",
                    type="left_outer",
                    relationship="many_to_one",
                ),
            ],
        )
        self.assertEqual(explore.name, "order_items")
        self.assertEqual(len(explore.joins), 1)
        self.assertEqual(explore.joins[0].relationship, "many_to_one")


class TestPbiModels(unittest.TestCase):
    """Test Power BI model classes."""

    def test_pbi_column_creation(self):
        """Test creating a PBI column."""
        col = PbiColumn(
            name="CustomerID",
            data_type=PbiDataType.STRING,
            source_column="customer_id",
        )
        self.assertEqual(col.name, "CustomerID")
        self.assertEqual(col.data_type, PbiDataType.STRING)

    def test_pbi_measure_creation(self):
        """Test creating a PBI measure."""
        measure = PbiMeasure(
            name="Total Sales",
            expression="SUM('Orders'[SalesAmount])",
            format_string="#,##0.00",
        )
        self.assertEqual(measure.name, "Total Sales")
        self.assertIn("SUM", measure.expression)

    def test_pbi_table_creation(self):
        """Test creating a PBI table."""
        table = PbiTable(
            name="Orders",
            columns=[
                PbiColumn(name="OrderID", data_type=PbiDataType.INT64),
                PbiColumn(name="CustomerID", data_type=PbiDataType.STRING),
            ],
            measures=[
                PbiMeasure(name="Order Count", expression="COUNTROWS('Orders')"),
            ],
        )
        self.assertEqual(table.name, "Orders")
        self.assertEqual(len(table.columns), 2)
        self.assertEqual(len(table.measures), 1)


class TestExpressionConverter(unittest.TestCase):
    """Test expression conversion from Looker to DAX."""

    def setUp(self):
        self.converter = ExpressionConverter()

    def test_simple_sum_measure(self):
        """Test converting a simple SUM measure."""
        measure = LookmlMeasure(
            name="total_amount",
            type="sum",
            sql="${TABLE}.amount",
        )
        result = self.converter.convert_measure(measure, "orders", "Orders")
        self.assertTrue(result.success)
        self.assertIn("SUM", result.expression)

    def test_count_distinct_measure(self):
        """Test converting COUNT_DISTINCT measure."""
        measure = LookmlMeasure(
            name="unique_customers",
            type="count_distinct",
            sql="${TABLE}.customer_id",
        )
        result = self.converter.convert_measure(measure, "orders", "Orders")
        self.assertTrue(result.success)
        self.assertIn("DISTINCTCOUNT", result.expression)

    def test_count_measure(self):
        """Test converting COUNT measure."""
        measure = LookmlMeasure(
            name="order_count",
            type="count",
        )
        result = self.converter.convert_measure(measure, "orders", "Orders")
        self.assertTrue(result.success)
        self.assertIn("COUNTROWS", result.expression)

    def test_average_measure(self):
        """Test converting AVERAGE measure."""
        measure = LookmlMeasure(
            name="avg_price",
            type="average",
            sql="${TABLE}.price",
        )
        result = self.converter.convert_measure(measure, "products", "Products")
        self.assertTrue(result.success)
        self.assertIn("AVERAGE", result.expression)

    def test_table_placeholder_measure_reference(self):
        """Test ${TABLE}.field is converted to valid DAX column reference."""
        measure = LookmlMeasure(
            name="avg_total",
            type="average",
            sql="${TABLE}.total_amount",
        )
        result = self.converter.convert_measure(measure, "orders", "Orders")
        self.assertTrue(result.success)
        self.assertEqual(result.expression, "AVERAGE(Orders[TotalAmount])")

    def test_number_measure_with_sql(self):
        """Test converting number measure with SQL expression."""
        measure = LookmlMeasure(
            name="profit_margin",
            type="number",
            sql="${total_revenue} - ${total_cost}",
        )
        result = self.converter.convert_measure(measure, "financials", "Financials")
        self.assertTrue(result.success)

    def test_field_reference_conversion(self):
        """Test converting Looker field references to DAX."""
        # ${view.field} -> 'Table'[Field]
        result = self.converter.convert_field_reference(
            "${orders.amount}",
            {"orders": "Orders"}
        )
        self.assertIn("'Orders'", result)
        self.assertIn("[Amount]", result)


class TestDatatypeMapper(unittest.TestCase):
    """Test data type mapping between Looker and Power BI."""

    def test_string_mapping(self):
        """Test string type mapping."""
        result = DatatypeMapper.looker_to_pbi(LookerDataType.STRING)
        self.assertEqual(result, PbiDataType.STRING)

    def test_number_mapping(self):
        """Test number type mapping."""
        result = DatatypeMapper.looker_to_pbi(LookerDataType.NUMBER)
        self.assertEqual(result, PbiDataType.DOUBLE)

    def test_int_mapping(self):
        """Test integer type mapping."""
        result = DatatypeMapper.looker_to_pbi(LookerDataType.INT)
        self.assertEqual(result, PbiDataType.INT64)

    def test_date_mapping(self):
        """Test date type mapping."""
        result = DatatypeMapper.looker_to_pbi(LookerDataType.DATE)
        self.assertEqual(result, PbiDataType.DATETIME)

    def test_yesno_mapping(self):
        """Test yes/no (boolean) type mapping."""
        result = DatatypeMapper.looker_to_pbi(LookerDataType.YESNO)
        self.assertEqual(result, PbiDataType.BOOLEAN)


class TestJoinConverter(unittest.TestCase):
    """Test join conversion from Looker to Power BI relationships."""

    def setUp(self):
        self.converter = JoinConverter()

    def test_simple_join_conversion(self):
        """Test converting a simple join."""
        join = LookmlJoin(
            name="customers",
            sql_on="${orders.customer_id} = ${customers.id}",
            type="left_outer",
            relationship="many_to_one",
        )
        result = self.converter.convert(
            join,
            from_table="Orders",
            view_mapping={"orders": "Orders", "customers": "Customers"},
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.from_table, "Orders")
        self.assertEqual(result.to_table, "Customers")

    def test_relationship_cardinality(self):
        """Test relationship cardinality conversion."""
        # many_to_one
        join = LookmlJoin(
            name="products",
            sql_on="${order_items.product_id} = ${products.id}",
            relationship="many_to_one",
        )
        result = self.converter.convert(
            join,
            from_table="OrderItems",
            view_mapping={"order_items": "OrderItems", "products": "Products"},
        )
        self.assertIsNotNone(result)

    def test_chained_join_direction_uses_joined_view_as_one_side(self):
        """Test chained join keeps joined table on the target side."""
        join = LookmlJoin(
            name="customers",
            sql_on="${orders.customer_id} = ${customers.customer_id}",
            relationship="many_to_one",
        )
        result = self.converter.convert(
            join,
            base_view="order_items",
            view_to_table={
                "order_items": "OrderItems",
                "orders": "Orders",
                "customers": "Customers",
            },
        )
        self.assertIsNotNone(result)
        self.assertEqual(result.from_table, "Orders")
        self.assertEqual(result.to_table, "Customers")
        self.assertEqual(result.from_column, "customer_id")
        self.assertEqual(result.to_column, "customer_id")


class TestModelGenerator(unittest.TestCase):
    """Test Power BI model generation from Looker structures."""

    def setUp(self):
        self.generator = ModelGenerator()

    def test_generate_from_view(self):
        """Test generating PBI model from a single view."""
        view = LookmlView(
            name="customers",
            sql_table_name="public.customers",
            dimensions=[
                LookmlDimension(
                    name="customer_id",
                    type=LookerDataType.NUMBER,
                    sql="${TABLE}.id",
                    primary_key=True,
                ),
                LookmlDimension(
                    name="customer_name",
                    type=LookerDataType.STRING,
                    sql="${TABLE}.name",
                ),
            ],
            measures=[
                LookmlMeasure(
                    name="customer_count",
                    type="count",
                ),
            ],
        )

        model = self.generator.generate_from_view(view, model_name="CustomerModel")
        self.assertIsNotNone(model)
        self.assertEqual(model.name, "CustomerModel")
        self.assertEqual(len(model.tables), 1)
        self.assertEqual(model.tables[0].name, "Customers")

    def test_generate_from_project(self):
        """Test generating PBI model from a Looker project."""
        project = LookmlProject(
            name="ecommerce",
            views=[
                LookmlView(
                    name="orders",
                    sql_table_name="public.orders",
                    dimensions=[
                        LookmlDimension(name="order_id", type=LookerDataType.NUMBER),
                    ],
                ),
                LookmlView(
                    name="products",
                    sql_table_name="public.products",
                    dimensions=[
                        LookmlDimension(name="product_id", type=LookerDataType.NUMBER),
                    ],
                ),
            ],
            models=[
                LookmlModel(
                    name="ecommerce",
                    explores=[
                        LookmlExplore(name="orders", from_view="orders"),
                    ],
                ),
            ],
        )

        model = self.generator.generate_from_project(project)
        self.assertIsNotNone(model)
        self.assertEqual(len(model.tables), 2)

    def test_generate_from_project_unique_measure_names(self):
        """Test duplicate measure names are made unique across model tables."""
        project = LookmlProject(
            name="measure_collision",
            views=[
                LookmlView(
                    name="orders",
                    measures=[LookmlMeasure(name="count", type="count")],
                ),
                LookmlView(
                    name="customers",
                    measures=[LookmlMeasure(name="count", type="count")],
                ),
            ],
            models=[LookmlModel(name="measure_collision", explores=[])],
        )

        model = self.generator.generate_from_project(project)
        measure_names = []
        for table in model.tables:
            for measure in table.measures:
                measure_names.append(measure.name)

        self.assertEqual(len(measure_names), 2)
        self.assertEqual(len(set(m.lower() for m in measure_names)), 2)
        self.assertIn("Count", measure_names)
        self.assertIn("Customers_Count", measure_names)

    def test_generate_from_project_uses_connection_for_partition_source(self):
        """Test partition source connector follows model connection type."""
        project = LookmlProject(
            name="snowflake_project",
            connection="snowflake",
            views=[
                LookmlView(
                    name="orders",
                    sql_table_name="ecomm.order_items",
                    dimensions=[LookmlDimension(name="id", type=LookerDataType.NUMBER)],
                ),
            ],
            models=[LookmlModel(name="snowflake_project", explores=[])],
        )

        model = self.generator.generate_from_project(project)
        self.assertEqual(len(model.tables), 1)
        self.assertEqual(len(model.tables[0].partitions), 1)
        expression = model.tables[0].partitions[0].expression
        self.assertIn("Snowflake.Databases", expression)
        self.assertIn('FROM ""ecomm"".""order_items""', expression)

    def test_generate_from_project_maps_lookerdata_to_bigquery(self):
        """Test legacy lookerdata connection name maps to BigQuery connector."""
        project = LookmlProject(
            name="bq_project",
            connection="lookerdata",
            views=[
                LookmlView(
                    name="orders",
                    sql_table_name="healthcare_demo_live.realtime_observation",
                    dimensions=[LookmlDimension(name="id", type=LookerDataType.NUMBER)],
                ),
            ],
            models=[LookmlModel(name="bq_project", explores=[])],
        )

        model = self.generator.generate_from_project(project)
        expression = model.tables[0].partitions[0].expression
        self.assertIn("GoogleBigQuery.Database", expression)
        self.assertIn('Dataset = Project{[Name="healthcare_demo_live"]}[Data]', expression)
        self.assertIn('Table = Dataset{[Name="realtime_observation"]}[Data]', expression)
        self.assertNotIn("Value.NativeQuery", expression)

    def test_generate_from_project_local_csv_partition(self):
        """Test local CSV connection produces file-based partition expressions."""
        project = LookmlProject(
            name="local_csv_project",
            connection="local_csv",
            views=[
                LookmlView(
                    name="order_items",
                    sql_table_name="ecomm.order_items",
                    dimensions=[LookmlDimension(name="id", type=LookerDataType.NUMBER)],
                ),
            ],
            models=[LookmlModel(name="local_csv_project", explores=[])],
        )

        model = self.generator.generate_from_project(project)
        expression = model.tables[0].partitions[0].expression
        self.assertIn("Csv.Document(File.Contents(", expression)
        self.assertIn("order_items.csv", expression)

    def test_generate_from_project_bigquery_backtick_table_reference(self):
        """Test BigQuery backtick-qualified table names navigate project/dataset/table."""
        project = LookmlProject(
            name="bq_backtick_project",
            connection="bigquery",
            views=[
                LookmlView(
                    name="users",
                    sql_table_name="`bigquery-public-data.thelook_ecommerce.users`",
                    dimensions=[LookmlDimension(name="id", type=LookerDataType.NUMBER)],
                ),
            ],
            models=[LookmlModel(name="bq_backtick_project", explores=[])],
        )

        model = self.generator.generate_from_project(project)
        expression = model.tables[0].partitions[0].expression
        self.assertIn('Project = Source{[Name="bigquery-public-data"]}[Data]', expression)
        self.assertIn('Dataset = Project{[Name="thelook_ecommerce"]}[Data]', expression)
        self.assertIn('Table = Dataset{[Name="users"]}[Data]', expression)

    def test_generate_from_project_bigquery_derived_table_uses_project_native_query(self):
        """Test BigQuery derived table native query binds to project node, not root source."""
        project = LookmlProject(
            name="bq_derived_project",
            connection="bigquery",
            views=[
                LookmlView(
                    name="session_facts",
                    derived_table={
                        "sql": (
                            "SELECT session_id "
                            "FROM `bigquery-public-data.thelook_ecommerce.events`"
                        )
                    },
                    dimensions=[LookmlDimension(name="session_id", type=LookerDataType.STRING)],
                ),
            ],
            models=[LookmlModel(name="bq_derived_project", explores=[])],
        )

        model = self.generator.generate_from_project(project)
        expression = model.tables[0].partitions[0].expression
        self.assertIn('Project = Source{[Name="bigquery-public-data"]}[Data]', expression)
        self.assertIn("Query = Value.NativeQuery(Project", expression)


class TestProjectParser(unittest.TestCase):
    """Test project parsing behavior for model-level metadata."""

    def test_parse_extracts_top_level_model_connection(self):
        """Top-level `connection:` in .model.lkml should populate project connection."""
        parser = ProjectParser()

        with tempfile.TemporaryDirectory() as tmpdir:
            project_dir = Path(tmpdir)
            model_file = project_dir / "sample.model.lkml"
            view_file = project_dir / "sample.view.lkml"

            model_file.write_text(
                'connection: "snowflake"\n'
                'label: "Sample"\n'
                'include: "/views/*.view"\n'
                "explore: sample {}\n",
                encoding="utf-8",
            )
            view_file.write_text(
                "view: sample {\n"
                "  sql_table_name: analytics.sample ;;\n"
                "  dimension: id { type: number sql: ${TABLE}.id ;; }\n"
                "}\n",
                encoding="utf-8",
            )

            project = parser.parse(project_dir)

        self.assertEqual(project.connection, "snowflake")
        self.assertEqual(len(project.models), 1)
        self.assertEqual(project.models[0].connection, "snowflake")

    def test_parse_derived_table_without_name(self):
        """`derived_table: { ... }` should be parsed even when unnamed."""
        parser = LookmlParser()
        content = """
view: event_session_facts {
  derived_table: {
    sql:
      SELECT session_id
      FROM `bigquery-public-data.thelook_ecommerce.events`
    ;;
  }
  dimension: session_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.session_id ;;
  }
}
"""
        views = parser.parse_content(content)

        self.assertEqual(len(views), 1)
        self.assertIsNotNone(views[0].derived_table)
        self.assertIn("SELECT session_id", views[0].derived_table.get("sql", ""))


class TestTmdlGenerator(unittest.TestCase):
    """Test TMDL file generation."""

    def setUp(self):
        self.generator = TmdlGenerator()

    def test_generate_tmdl_files(self):
        """Test generating TMDL files from PBI model."""
        model = PbiModel(
            name="TestModel",
            tables=[
                PbiTable(
                    name="Orders",
                    columns=[
                        PbiColumn(name="OrderID", data_type=PbiDataType.INT64),
                        PbiColumn(name="Amount", data_type=PbiDataType.DOUBLE),
                    ],
                    measures=[
                        PbiMeasure(
                            name="Total Amount",
                            expression="SUM('Orders'[Amount])",
                        ),
                    ],
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            files = self.generator.generate(model, tmpdir)

            self.assertTrue(len(files) > 0)

            # Check model.tmdl exists
            model_file = Path(tmpdir) / "model.tmdl"
            self.assertTrue(model_file.exists())

            # Check definition/database.tmdl exists
            db_file = Path(tmpdir) / "definition" / "database.tmdl"
            self.assertTrue(db_file.exists())

    def test_relationship_names_are_made_unique(self):
        """Test duplicate relationship names are made unique in generated TMDL."""
        model = PbiModel(
            name="RelationshipModel",
            tables=[
                PbiTable(
                    name="Orders",
                    columns=[PbiColumn(name="id", data_type=PbiDataType.INT64)],
                ),
                PbiTable(
                    name="Facts",
                    columns=[
                        PbiColumn(name="order_id", data_type=PbiDataType.INT64),
                        PbiColumn(name="next_order_id", data_type=PbiDataType.INT64),
                    ],
                ),
            ],
            relationships=[
                PbiRelationship(
                    name="Orders_Facts",
                    from_table="Orders",
                    from_column="id",
                    to_table="Facts",
                    to_column="order_id",
                    cardinality=Cardinality.ONE_TO_MANY,
                    cross_filter_direction=CrossFilterDirection.SINGLE,
                ),
                PbiRelationship(
                    name="Orders_Facts",
                    from_table="Orders",
                    from_column="id",
                    to_table="Facts",
                    to_column="next_order_id",
                    cardinality=Cardinality.ONE_TO_MANY,
                    cross_filter_direction=CrossFilterDirection.SINGLE,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            self.generator.generate(model, tmpdir)
            relationships_file = Path(tmpdir) / "pbit" / "Model" / "relationships.tmdl"
            self.assertTrue(relationships_file.exists())
            content = relationships_file.read_text(encoding="utf-8")

            self.assertIn("relationship Orders_Facts", content)
            self.assertIn("relationship Orders_Facts_2", content)

    def test_relationship_columns_resolve_from_source_names(self):
        """Test relationships resolve source column names to generated column names."""
        model = PbiModel(
            name="RelationshipResolutionModel",
            tables=[
                PbiTable(
                    name="Orders",
                    columns=[
                        PbiColumn(name="OrderId", data_type=PbiDataType.INT64, source_column="order_id"),
                    ],
                ),
                PbiTable(
                    name="Facts",
                    columns=[
                        PbiColumn(name="OrderId", data_type=PbiDataType.INT64, source_column="order_id"),
                    ],
                ),
            ],
            relationships=[
                PbiRelationship(
                    name="Orders_Facts",
                    from_table="orders",
                    from_column="order_id",
                    to_table="facts",
                    to_column="order_id",
                    cardinality=Cardinality.ONE_TO_MANY,
                    cross_filter_direction=CrossFilterDirection.SINGLE,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            self.generator.generate(model, tmpdir)
            relationships_file = Path(tmpdir) / "pbit" / "Model" / "relationships.tmdl"
            content = relationships_file.read_text(encoding="utf-8")

            self.assertIn("fromColumn: Orders.OrderId", content)
            self.assertIn("toColumn: Facts.OrderId", content)

    def test_duplicate_columns_are_skipped(self):
        """Test duplicate column names are not emitted twice in table TMDL."""
        model = PbiModel(
            name="DuplicateColumnsModel",
            tables=[
                PbiTable(
                    name="Observation",
                    columns=[
                        PbiColumn(name="Effective", data_type=PbiDataType.DATETIME),
                        PbiColumn(name="Effective", data_type=PbiDataType.STRING),
                    ],
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            self.generator.generate(model, tmpdir)
            table_file = Path(tmpdir) / "pbit" / "Model" / "tables" / "Observation.tmdl"
            content = table_file.read_text(encoding="utf-8")

            self.assertEqual(content.count("column 'Effective'"), 1)


class TestEndToEndMigration(unittest.TestCase):
    """End-to-end migration tests."""

    def test_migrate_simple_view_content(self):
        """Test migrating simple view content."""
        lookml_content = '''
view: customers {
  sql_table_name: public.customers ;;

  dimension: customer_id {
    type: number
    sql: ${TABLE}.id ;;
    primary_key: yes
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  measure: count {
    type: count
  }
}
'''
        parser = LookmlParser()
        views = parser.parse_content(lookml_content)

        self.assertEqual(len(views), 1)
        self.assertEqual(views[0].name, "customers")

        # Generate model
        generator = ModelGenerator()
        model = generator.generate_from_view(views[0], "CustomerModel")

        self.assertEqual(model.name, "CustomerModel")
        self.assertEqual(len(model.tables), 1)

    def test_complete_migration_flow(self):
        """Test complete migration with all components."""
        # Create test project structure
        project = LookmlProject(
            name="test_project",
            views=[
                LookmlView(
                    name="sales",
                    sql_table_name="dbo.sales",
                    dimensions=[
                        LookmlDimension(
                            name="sale_id",
                            type=LookerDataType.NUMBER,
                            sql="${TABLE}.id",
                            primary_key=True,
                        ),
                        LookmlDimension(
                            name="sale_date",
                            type=LookerDataType.DATE,
                            sql="${TABLE}.sale_date",
                        ),
                        LookmlDimension(
                            name="amount",
                            type=LookerDataType.NUMBER,
                            sql="${TABLE}.amount",
                        ),
                    ],
                    measures=[
                        LookmlMeasure(
                            name="total_sales",
                            type="sum",
                            sql="${TABLE}.amount",
                        ),
                        LookmlMeasure(
                            name="sale_count",
                            type="count",
                        ),
                        LookmlMeasure(
                            name="avg_sale",
                            type="average",
                            sql="${TABLE}.amount",
                        ),
                    ],
                ),
            ],
            models=[
                LookmlModel(
                    name="sales_model",
                    explores=[
                        LookmlExplore(name="sales", from_view="sales"),
                    ],
                ),
            ],
        )

        # Generate PBI model
        model_gen = ModelGenerator()
        pbi_model = model_gen.generate_from_project(project, model_name="SalesModel")

        # Generate TMDL files
        tmdl_gen = TmdlGenerator()

        with tempfile.TemporaryDirectory() as tmpdir:
            files = tmdl_gen.generate(pbi_model, tmpdir)

            # Verify output
            self.assertTrue(len(files) >= 2)  # At least model.tmdl and database.tmdl

            # Check content
            model_content = (Path(tmpdir) / "model.tmdl").read_text()
            self.assertIn("SalesModel", model_content)


class TestMigrationResult(unittest.TestCase):
    """Test migration result handling."""

    def test_successful_result(self):
        """Test successful migration result properties."""
        from looker_migrator.models import MigrationResult

        result = MigrationResult(
            success=True,
            output_path="/tmp/output",
            source_file="/path/to/project",
            model_name="TestModel",
            tables_count=5,
            measures_count=10,
            relationships_count=3,
            views_converted=5,
            explores_converted=2,
            generated_files=["model.tmdl", "database.tmdl"],
        )

        self.assertTrue(result.success)
        self.assertEqual(result.tables_count, 5)
        self.assertEqual(result.measures_count, 10)

    def test_failed_result(self):
        """Test failed migration result."""
        from looker_migrator.models import MigrationResult, MigrationError

        result = MigrationResult(
            success=False,
            output_path="/tmp/output",
            source_file="/path/to/project",
            errors=[
                MigrationError(
                    code="PARSE_ERROR",
                    message="Failed to parse view file",
                    source_element="views/broken.view.lkml",
                ),
            ],
        )

        self.assertFalse(result.success)
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.errors[0].code, "PARSE_ERROR")


if __name__ == "__main__":
    unittest.main()
