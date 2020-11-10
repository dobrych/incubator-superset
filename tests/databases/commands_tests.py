# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=no-self-use, invalid-name

from unittest.mock import patch

import pytest
import yaml

from superset import db, security_manager
from superset.commands.exceptions import CommandInvalidError
from superset.commands.importers.exceptions import IncorrectVersionError
from superset.connectors.sqla.models import SqlaTable
from superset.databases.commands.exceptions import DatabaseNotFoundError
from superset.databases.commands.export import ExportDatabasesCommand
from superset.databases.commands.importers.v1 import ImportDatabasesCommand
from superset.datasets.commands.importers.v1 import ImportDatasetsCommand
from superset.models.core import Database
from superset.utils.core import backend, get_example_database
from tests.base_tests import SupersetTestCase
from tests.fixtures.importexport import (
    database_config,
    dataset_config,
    metadata_config,
)


class TestExportDatabasesCommand(SupersetTestCase):
    @patch("superset.security.manager.g")
    def test_export_database_command(self, mock_g):
        mock_g.user = security_manager.find_user("admin")

        example_db = get_example_database()
        command = ExportDatabasesCommand([example_db.id])
        contents = dict(command.run())

        # TODO: this list shouldn't depend on the order in which unit tests are run
        # or on the backend; for now use a stable subset
        core_files = {
            "metadata.yaml",
            "databases/examples.yaml",
            "datasets/examples/energy_usage.yaml",
            "datasets/examples/wb_health_population.yaml",
            "datasets/examples/birth_names.yaml",
        }
        expected_extra = {
            "engine_params": {},
            "metadata_cache_timeout": {},
            "metadata_params": {},
            "schemas_allowed_for_csv_upload": [],
        }
        if backend() == "presto":
            expected_extra = {"engine_params": {"connect_args": {"poll_interval": 0.1}}}

        assert core_files.issubset(set(contents.keys()))

        metadata = yaml.safe_load(contents["databases/examples.yaml"])
        assert metadata == (
            {
                "allow_csv_upload": True,
                "allow_ctas": True,
                "allow_cvas": True,
                "allow_run_async": False,
                "cache_timeout": None,
                "database_name": "examples",
                "expose_in_sqllab": True,
                "extra": expected_extra,
                "sqlalchemy_uri": example_db.sqlalchemy_uri,
                "uuid": str(example_db.uuid),
                "version": "1.0.0",
            }
        )

        metadata = yaml.safe_load(contents["datasets/examples/birth_names.yaml"])
        metadata.pop("uuid")
        assert metadata == {
            "table_name": "birth_names",
            "main_dttm_col": None,
            "description": "Adding a DESCRip",
            "default_endpoint": "",
            "offset": 66,
            "cache_timeout": 55,
            "schema": "",
            "sql": "",
            "params": None,
            "template_params": None,
            "filter_select_enabled": True,
            "fetch_values_predicate": None,
            "extra": None,
            "metrics": [
                {
                    "metric_name": "ratio",
                    "verbose_name": "Ratio Boys/Girls",
                    "metric_type": None,
                    "expression": "sum(sum_boys) / sum(sum_girls)",
                    "description": "This represents the ratio of boys/girls",
                    "d3format": ".2%",
                    "extra": None,
                    "warning_text": "no warning",
                },
                {
                    "metric_name": "sum__num",
                    "verbose_name": "Babies",
                    "metric_type": None,
                    "expression": "SUM(num)",
                    "description": "",
                    "d3format": "",
                    "extra": None,
                    "warning_text": "",
                },
                {
                    "metric_name": "count",
                    "verbose_name": "",
                    "metric_type": None,
                    "expression": "count(1)",
                    "description": None,
                    "d3format": None,
                    "extra": None,
                    "warning_text": None,
                },
            ],
            "columns": [
                {
                    "column_name": "num_california",
                    "verbose_name": None,
                    "is_dttm": False,
                    "is_active": None,
                    "type": "NUMBER",
                    "groupby": False,
                    "filterable": False,
                    "expression": "CASE WHEN state = 'CA' THEN num ELSE 0 END",
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "ds",
                    "verbose_name": "",
                    "is_dttm": True,
                    "is_active": None,
                    "type": "DATETIME",
                    "groupby": True,
                    "filterable": True,
                    "expression": "",
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "sum_girls",
                    "verbose_name": None,
                    "is_dttm": False,
                    "is_active": None,
                    "type": "BIGINT(20)",
                    "groupby": False,
                    "filterable": False,
                    "expression": "",
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "gender",
                    "verbose_name": None,
                    "is_dttm": False,
                    "is_active": None,
                    "type": "VARCHAR(16)",
                    "groupby": True,
                    "filterable": True,
                    "expression": "",
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "state",
                    "verbose_name": None,
                    "is_dttm": None,
                    "is_active": None,
                    "type": "VARCHAR(10)",
                    "groupby": True,
                    "filterable": True,
                    "expression": None,
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "sum_boys",
                    "verbose_name": None,
                    "is_dttm": None,
                    "is_active": None,
                    "type": "BIGINT(20)",
                    "groupby": True,
                    "filterable": True,
                    "expression": None,
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "num",
                    "verbose_name": None,
                    "is_dttm": None,
                    "is_active": None,
                    "type": "BIGINT(20)",
                    "groupby": True,
                    "filterable": True,
                    "expression": None,
                    "description": None,
                    "python_date_format": None,
                },
                {
                    "column_name": "name",
                    "verbose_name": None,
                    "is_dttm": None,
                    "is_active": None,
                    "type": "VARCHAR(255)",
                    "groupby": True,
                    "filterable": True,
                    "expression": None,
                    "description": None,
                    "python_date_format": None,
                },
            ],
            "version": "1.0.0",
            "database_uuid": str(example_db.uuid),
        }

    @patch("superset.security.manager.g")
    def test_export_database_command_no_access(self, mock_g):
        """Test that users can't export databases they don't have access to"""
        mock_g.user = security_manager.find_user("gamma")

        example_db = get_example_database()
        command = ExportDatabasesCommand([example_db.id])
        contents = command.run()
        with self.assertRaises(DatabaseNotFoundError):
            next(contents)

    @patch("superset.security.manager.g")
    def test_export_database_command_invalid_database(self, mock_g):
        """Test that an error is raised when exporting an invalid database"""
        mock_g.user = security_manager.find_user("admin")
        command = ExportDatabasesCommand([-1])
        contents = command.run()
        with self.assertRaises(DatabaseNotFoundError):
            next(contents)

    @patch("superset.security.manager.g")
    def test_export_database_command_key_order(self, mock_g):
        """Test that they keys in the YAML have the same order as export_fields"""
        mock_g.user = security_manager.find_user("admin")

        example_db = get_example_database()
        command = ExportDatabasesCommand([example_db.id])
        contents = dict(command.run())

        metadata = yaml.safe_load(contents["databases/examples.yaml"])
        assert list(metadata.keys()) == [
            "database_name",
            "sqlalchemy_uri",
            "cache_timeout",
            "expose_in_sqllab",
            "allow_run_async",
            "allow_ctas",
            "allow_cvas",
            "allow_csv_upload",
            "extra",
            "uuid",
            "version",
        ]

    def test_import_v1_database(self):
        """Test that a database can be imported"""
        contents = {
            "metadata.yaml": yaml.safe_dump(metadata_config),
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
        }
        command = ImportDatabasesCommand(contents)
        command.run()

        database = (
            db.session.query(Database).filter_by(uuid=database_config["uuid"]).one()
        )
        assert database.allow_csv_upload
        assert database.allow_ctas
        assert database.allow_cvas
        assert not database.allow_run_async
        assert database.cache_timeout is None
        assert database.database_name == "imported_database"
        assert database.expose_in_sqllab
        assert database.extra == "{}"
        assert database.sqlalchemy_uri == "sqlite:///test.db"

        db.session.delete(database)
        db.session.commit()

    def test_import_v1_database_multiple(self):
        """Test that a database can be imported multiple times"""
        num_databases = db.session.query(Database).count()

        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "metadata.yaml": yaml.safe_dump(metadata_config),
        }
        command = ImportDatabasesCommand(contents)

        # import twice
        command.run()
        command.run()

        database = (
            db.session.query(Database).filter_by(uuid=database_config["uuid"]).one()
        )
        assert database.allow_csv_upload

        # update allow_csv_upload to False
        new_config = database_config.copy()
        new_config["allow_csv_upload"] = False
        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(new_config),
            "metadata.yaml": yaml.safe_dump(metadata_config),
        }
        command = ImportDatabasesCommand(contents)
        command.run()

        database = (
            db.session.query(Database).filter_by(uuid=database_config["uuid"]).one()
        )
        assert not database.allow_csv_upload

        # test that only one database was created
        new_num_databases = db.session.query(Database).count()
        assert new_num_databases == num_databases + 1

        db.session.delete(database)
        db.session.commit()

    def test_import_v1_database_with_dataset(self):
        """Test that a database can be imported with datasets"""
        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(dataset_config),
            "metadata.yaml": yaml.safe_dump(metadata_config),
        }
        command = ImportDatabasesCommand(contents)
        command.run()

        database = (
            db.session.query(Database).filter_by(uuid=database_config["uuid"]).one()
        )
        assert len(database.tables) == 1
        assert str(database.tables[0].uuid) == "10808100-158b-42c4-842e-f32b99d88dfb"

        db.session.delete(database.tables[0])
        db.session.delete(database)
        db.session.commit()

    def test_import_v1_database_with_dataset_multiple(self):
        """Test that a database can be imported multiple times w/o changing datasets"""
        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(dataset_config),
            "metadata.yaml": yaml.safe_dump(metadata_config),
        }
        command = ImportDatabasesCommand(contents)
        command.run()

        dataset = (
            db.session.query(SqlaTable).filter_by(uuid=dataset_config["uuid"]).one()
        )
        assert dataset.offset == 66

        new_config = dataset_config.copy()
        new_config["offset"] = 67
        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(new_config),
            "metadata.yaml": yaml.safe_dump(metadata_config),
        }
        command = ImportDatabasesCommand(contents)
        command.run()

        # the underlying dataset should not be modified by the second import, since
        # we're importing a database, not a dataset
        dataset = (
            db.session.query(SqlaTable).filter_by(uuid=dataset_config["uuid"]).one()
        )
        assert dataset.offset == 66

        db.session.delete(dataset)
        db.session.delete(dataset.database)
        db.session.commit()

    def test_import_v1_database_validation(self):
        """Test different validations applied when importing a database"""
        # metadata.yaml must be present
        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
        }
        command = ImportDatabasesCommand(contents)
        with pytest.raises(IncorrectVersionError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing model"
        assert excinfo.value.normalized_messages() == {
            "metadata.yaml": "File is missing"
        }

        # version should be 1.0.0
        contents["metadata.yaml"] = yaml.safe_dump(
            {
                "version": "2.0.0",
                "type": "Database",
                "timestamp": "2020-11-04T21:27:44.423819+00:00",
            }
        )
        command = ImportDatabasesCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing model"
        assert excinfo.value.normalized_messages() == {
            "metadata.yaml": {
                "version": ["Must be equal to 1.0.0."],
            },
        }

        # type should be Database
        contents["metadata.yaml"] = yaml.safe_dump(
            {
                "version": "1.0.0",
                "type": "Dataset",
                "timestamp": "2020-11-04T21:27:44.423819+00:00",
            }
        )
        command = ImportDatabasesCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing model"
        assert excinfo.value.normalized_messages() == {
            "metadata.yaml": {
                "type": ["Must be equal to Database."],
            },
        }

        # must also validate datasets
        broken_config = dataset_config.copy()
        del broken_config["table_name"]
        contents["metadata.yaml"] = yaml.safe_dump(metadata_config)
        contents["datasets/imported_dataset.yaml"] = yaml.safe_dump(broken_config)
        command = ImportDatabasesCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing associated dataset"
        assert excinfo.value.normalized_messages() == {
            "datasets/imported_dataset.yaml": {
                "table_name": ["Missing data for required field."],
            },
        }

    def test_import_v1_dataset(self):
        """Test that we can import a dataset"""
        dataset_metadata_config = metadata_config.copy()
        dataset_metadata_config["type"] = "SqlaTable"
        contents = {
            "metadata.yaml": yaml.safe_dump(dataset_metadata_config),
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(dataset_config),
        }
        command = ImportDatasetsCommand(contents)
        command.run()

        dataset = (
            db.session.query(SqlaTable).filter_by(uuid=dataset_config["uuid"]).one()
        )
        assert dataset.table_name == "imported_dataset"
        assert dataset.main_dttm_col is None
        assert dataset.description == "This is a dataset that was exported"
        assert dataset.default_endpoint == ""
        assert dataset.offset == 66
        assert dataset.cache_timeout == 55
        assert dataset.schema == ""
        assert dataset.sql == ""
        assert dataset.params is None
        assert dataset.template_params is None
        assert dataset.filter_select_enabled
        assert dataset.fetch_values_predicate is None
        assert dataset.extra is None

        # database is also imported
        assert str(dataset.database.uuid) == "b8a1ccd3-779d-4ab7-8ad8-9ab119d7fe89"

        assert len(dataset.metrics) == 1
        metric = dataset.metrics[0]
        assert metric.metric_name == "count"
        assert metric.verbose_name == ""
        assert metric.metric_type is None
        assert metric.expression == "count(1)"
        assert metric.description is None
        assert metric.d3format is None
        assert metric.extra is None
        assert metric.warning_text is None

        assert len(dataset.columns) == 1
        column = dataset.columns[0]
        assert column.column_name == "cnt"
        assert column.verbose_name == "Count of something"
        assert not column.is_dttm
        assert column.is_active  # imported columns are set to active
        assert column.type == "NUMBER"
        assert not column.groupby
        assert column.filterable
        assert column.expression == ""
        assert column.description is None
        assert column.python_date_format is None

        db.session.delete(dataset)
        db.session.delete(dataset.database)
        db.session.commit()

    def test_import_v1_dataset_multiple(self):
        """Test that a dataset can be imported multiple times"""
        dataset_metadata_config = metadata_config.copy()
        dataset_metadata_config["type"] = "SqlaTable"
        contents = {
            "metadata.yaml": yaml.safe_dump(dataset_metadata_config),
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(dataset_config),
        }
        command = ImportDatasetsCommand(contents)
        command.run()
        command.run()
        dataset = (
            db.session.query(SqlaTable).filter_by(uuid=dataset_config["uuid"]).one()
        )
        assert dataset.table_name == "imported_dataset"

        # test that columns and metrics sync, ie, old ones not the import
        # are removed
        new_config = dataset_config.copy()
        new_config["metrics"][0]["metric_name"] = "count2"
        new_config["columns"][0]["column_name"] = "cnt2"
        contents = {
            "metadata.yaml": yaml.safe_dump(dataset_metadata_config),
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(new_config),
        }
        command = ImportDatasetsCommand(contents)
        command.run()
        dataset = (
            db.session.query(SqlaTable).filter_by(uuid=dataset_config["uuid"]).one()
        )
        assert len(dataset.metrics) == 1
        assert dataset.metrics[0].metric_name == "count2"
        assert len(dataset.columns) == 1
        assert dataset.columns[0].column_name == "cnt2"

        db.session.delete(dataset)
        db.session.delete(dataset.database)
        db.session.commit()

    def test_import_v1_dataset_validation(self):
        """Test different validations applied when importing a dataset"""
        # metadata.yaml must be present
        contents = {
            "datasets/imported_dataset.yaml": yaml.safe_dump(dataset_config),
        }
        command = ImportDatasetsCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing model"
        assert excinfo.value.normalized_messages() == {
            "metadata.yaml": "File is missing"
        }

        # version should be 1.0.0
        contents["metadata.yaml"] = yaml.safe_dump(
            {
                "version": "2.0.0",
                "type": "SqlaTable",
                "timestamp": "2020-11-04T21:27:44.423819+00:00",
            }
        )
        command = ImportDatasetsCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing model"
        assert excinfo.value.normalized_messages() == {
            "metadata.yaml": {
                "version": ["Must be equal to 1.0.0."],
            },
        }

        # type should be SqlaTable (from the model)
        contents["metadata.yaml"] = yaml.safe_dump(
            {
                "version": "1.0.0",
                "type": "Database",
                "timestamp": "2020-11-04T21:27:44.423819+00:00",
            }
        )
        command = ImportDatasetsCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing model"
        assert excinfo.value.normalized_messages() == {
            "metadata.yaml": {
                "type": ["Must be equal to SqlaTable."],
            },
        }

        # must also validate databases
        dataset_metadata_config = metadata_config.copy()
        dataset_metadata_config["type"] = "SqlaTable"
        broken_config = database_config.copy()
        del broken_config["database_name"]
        contents["metadata.yaml"] = yaml.safe_dump(dataset_metadata_config)
        contents["databases/imported_database.yaml"] = yaml.safe_dump(broken_config)
        command = ImportDatasetsCommand(contents)
        with pytest.raises(CommandInvalidError) as excinfo:
            command.run()
        assert str(excinfo.value) == "Error importing associated database"
        assert excinfo.value.normalized_messages() == {
            "databases/imported_database.yaml": {
                "database_name": ["Missing data for required field."],
            },
        }

    @patch("superset.datasets.commands.importers.v1.ImportDatasetsCommand.import_")
    def test_import_v1_rollback(self, mock_import_dataset):
        """Test than on an exception everything is rolled back"""
        num_databases = db.session.query(Database).count()

        # raise an exception when importing the dataset, after the database has
        # already been imported
        mock_import_dataset.side_effect = Exception("A wild exception appears!")

        contents = {
            "databases/imported_database.yaml": yaml.safe_dump(database_config),
            "datasets/imported_dataset.yaml": yaml.safe_dump(dataset_config),
            "metadata.yaml": yaml.safe_dump(metadata_config),
        }
        command = ImportDatabasesCommand(contents)
        with pytest.raises(Exception) as excinfo:
            command.run()
        assert str(excinfo.value) == "A wild exception appears!"

        # verify that the database was not added
        new_num_databases = db.session.query(Database).count()
        assert new_num_databases == num_databases
