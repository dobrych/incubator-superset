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
import json
import logging
import time
from copy import copy
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from flask_babel import lazy_gettext as _
from sqlalchemy.orm import make_transient, Session
from werkzeug.utils import secure_filename

from superset import ConnectorRegistry, db
from superset.commands.importers.exceptions import IncorrectVersionError
from superset.commands.importers.v1.utils import IMPORT_VERSION
from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn
from superset.dashboards.commands.importers import v1
from superset.databases.commands.exceptions import DatabaseNotFoundError
from superset.datasets.commands.exceptions import DatasetNotFoundError
from superset.datasets.commands.importers.v0 import import_dataset
from superset.exceptions import DashboardImportException
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset.utils.dashboard_filter_scopes_converter import (
    convert_filter_scopes,
    copy_filter_scopes,
)

logger = logging.getLogger(__name__)


def import_chart(
    slc_to_import: Slice,
    slc_to_override: Optional[Slice],
    import_time: Optional[int] = None,
) -> int:
    """Inserts or overrides slc in the database.

    remote_id and import_time fields in params_dict are set to track the
    slice origin and ensure correct overrides for multiple imports.
    Slice.perm is used to find the datasources and connect them.

    :param Slice slc_to_import: Slice object to import
    :param Slice slc_to_override: Slice to replace, id matches remote_id
    :returns: The resulting id for the imported slice
    :rtype: int
    """
    session = db.session
    make_transient(slc_to_import)
    slc_to_import.dashboards = []
    slc_to_import.alter_params(remote_id=slc_to_import.id, import_time=import_time)

    slc_to_import = slc_to_import.copy()
    slc_to_import.reset_ownership()
    params = slc_to_import.params_dict
    datasource = ConnectorRegistry.get_datasource_by_name(
        session,
        slc_to_import.datasource_type,
        params["datasource_name"],
        params["schema"],
        params["database_name"],
    )
    slc_to_import.datasource_id = datasource.id  # type: ignore
    if slc_to_override:
        slc_to_override.override(slc_to_import)
        session.flush()
        return slc_to_override.id
    session.add(slc_to_import)
    logger.info("Final slice: %s", str(slc_to_import.to_json()))
    session.flush()
    return slc_to_import.id


def import_dashboard(
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    dashboard_to_import: Dashboard,
    import_time: Optional[int] = None,
) -> int:
    """Imports the dashboard from the object to the database.

    Once dashboard is imported, json_metadata field is extended and stores
    remote_id and import_time. It helps to decide if the dashboard has to
    be overridden or just copies over. Slices that belong to this
    dashboard will be wired to existing tables. This function can be used
    to import/export dashboards between multiple superset instances.
    Audit metadata isn't copied over.
    """

    def alter_positions(
        dashboard: Dashboard, old_to_new_slc_id_dict: Dict[int, int]
    ) -> None:
        """Updates slice_ids in the position json.

        Sample position_json data:
        {
            "DASHBOARD_VERSION_KEY": "v2",
            "DASHBOARD_ROOT_ID": {
                "type": "DASHBOARD_ROOT_TYPE",
                "id": "DASHBOARD_ROOT_ID",
                "children": ["DASHBOARD_GRID_ID"]
            },
            "DASHBOARD_GRID_ID": {
                "type": "DASHBOARD_GRID_TYPE",
                "id": "DASHBOARD_GRID_ID",
                "children": ["DASHBOARD_CHART_TYPE-2"]
            },
            "DASHBOARD_CHART_TYPE-2": {
                "type": "CHART",
                "id": "DASHBOARD_CHART_TYPE-2",
                "children": [],
                "meta": {
                    "width": 4,
                    "height": 50,
                    "chartId": 118
                }
            },
        }
        """
        position_data = json.loads(dashboard.position_json)
        position_json = position_data.values()
        for value in position_json:
            if (
                isinstance(value, dict)
                and value.get("meta")
                and value.get("meta", {}).get("chartId")
            ):
                old_slice_id = value["meta"]["chartId"]

                if old_slice_id in old_to_new_slc_id_dict:
                    value["meta"]["chartId"] = old_to_new_slc_id_dict[old_slice_id]
        dashboard.position_json = json.dumps(position_data)

    logger.info("Started import of the dashboard: %s", dashboard_to_import.to_json())
    session = db.session
    logger.info("Dashboard has %d slices", len(dashboard_to_import.slices))
    # copy slices object as Slice.import_slice will mutate the slice
    # and will remove the existing dashboard - slice association
    slices = copy(dashboard_to_import.slices)

    # Clearing the slug to avoid conflicts
    dashboard_to_import.slug = None

    old_json_metadata = json.loads(dashboard_to_import.json_metadata or "{}")
    old_to_new_slc_id_dict: Dict[int, int] = {}
    new_timed_refresh_immune_slices = []
    new_expanded_slices = {}
    new_filter_scopes = {}
    i_params_dict = dashboard_to_import.params_dict
    remote_id_slice_map = {
        slc.params_dict["remote_id"]: slc
        for slc in session.query(Slice).all()
        if "remote_id" in slc.params_dict
    }
    for slc in slices:
        logger.info(
            "Importing slice %s from the dashboard: %s",
            slc.to_json(),
            dashboard_to_import.dashboard_title,
        )
        remote_slc = remote_id_slice_map.get(slc.id)
        new_slc_id = import_chart(slc, remote_slc, import_time=import_time)
        old_to_new_slc_id_dict[slc.id] = new_slc_id
        # update json metadata that deals with slice ids
        new_slc_id_str = str(new_slc_id)
        old_slc_id_str = str(slc.id)
        if (
            "timed_refresh_immune_slices" in i_params_dict
            and old_slc_id_str in i_params_dict["timed_refresh_immune_slices"]
        ):
            new_timed_refresh_immune_slices.append(new_slc_id_str)
        if (
            "expanded_slices" in i_params_dict
            and old_slc_id_str in i_params_dict["expanded_slices"]
        ):
            new_expanded_slices[new_slc_id_str] = i_params_dict["expanded_slices"][
                old_slc_id_str
            ]

    # since PR #9109, filter_immune_slices and filter_immune_slice_fields
    # are converted to filter_scopes
    # but dashboard create from import may still have old dashboard filter metadata
    # here we convert them to new filter_scopes metadata first
    filter_scopes = {}
    if (
        "filter_immune_slices" in i_params_dict
        or "filter_immune_slice_fields" in i_params_dict
    ):
        filter_scopes = convert_filter_scopes(old_json_metadata, slices)

    if "filter_scopes" in i_params_dict:
        filter_scopes = old_json_metadata.get("filter_scopes")

    # then replace old slice id to new slice id:
    if filter_scopes:
        new_filter_scopes = copy_filter_scopes(
            old_to_new_slc_id_dict=old_to_new_slc_id_dict,
            old_filter_scopes=filter_scopes,
        )

    # override the dashboard
    existing_dashboard = None
    for dash in session.query(Dashboard).all():
        if (
            "remote_id" in dash.params_dict
            and dash.params_dict["remote_id"] == dashboard_to_import.id
        ):
            existing_dashboard = dash

    dashboard_to_import = dashboard_to_import.copy()
    dashboard_to_import.id = None
    dashboard_to_import.reset_ownership()
    # position_json can be empty for dashboards
    # with charts added from chart-edit page and without re-arranging
    if dashboard_to_import.position_json:
        alter_positions(dashboard_to_import, old_to_new_slc_id_dict)
    dashboard_to_import.alter_params(import_time=import_time)
    dashboard_to_import.remove_params(param_to_remove="filter_immune_slices")
    dashboard_to_import.remove_params(param_to_remove="filter_immune_slice_fields")
    if new_filter_scopes:
        dashboard_to_import.alter_params(filter_scopes=new_filter_scopes)
    if new_expanded_slices:
        dashboard_to_import.alter_params(expanded_slices=new_expanded_slices)
    if new_timed_refresh_immune_slices:
        dashboard_to_import.alter_params(
            timed_refresh_immune_slices=new_timed_refresh_immune_slices
        )

    new_slices = (
        session.query(Slice).filter(Slice.id.in_(old_to_new_slc_id_dict.values())).all()
    )

    if existing_dashboard:
        existing_dashboard.override(dashboard_to_import)
        existing_dashboard.slices = new_slices
        session.flush()
        return existing_dashboard.id

    dashboard_to_import.slices = new_slices
    session.add(dashboard_to_import)
    session.flush()
    return dashboard_to_import.id  # type: ignore


def decode_dashboards(  # pylint: disable=too-many-return-statements
    o: Dict[str, Any]
) -> Any:
    """
    Function to be passed into json.loads obj_hook parameter
    Recreates the dashboard object from a json representation.
    """
    from superset.connectors.druid.models import (
        DruidCluster,
        DruidColumn,
        DruidDatasource,
        DruidMetric,
    )

    if "__Dashboard__" in o:
        return Dashboard(**o["__Dashboard__"])
    if "__Slice__" in o:
        return Slice(**o["__Slice__"])
    if "__TableColumn__" in o:
        return TableColumn(**o["__TableColumn__"])
    if "__SqlaTable__" in o:
        return SqlaTable(**o["__SqlaTable__"])
    if "__SqlMetric__" in o:
        return SqlMetric(**o["__SqlMetric__"])
    if "__DruidCluster__" in o:
        return DruidCluster(**o["__DruidCluster__"])
    if "__DruidColumn__" in o:
        return DruidColumn(**o["__DruidColumn__"])
    if "__DruidDatasource__" in o:
        return DruidDatasource(**o["__DruidDatasource__"])
    if "__DruidMetric__" in o:
        return DruidMetric(**o["__DruidMetric__"])
    if "__datetime__" in o:
        return datetime.strptime(o["__datetime__"], "%Y-%m-%dT%H:%M:%S")

    return o


def import_dashboards(
    session: Session,
    content: str,
    database_id: Optional[int] = None,
    import_time: Optional[int] = None,
) -> None:
    """Imports dashboards from a stream to databases"""
    current_tt = int(time.time())
    import_time = current_tt if import_time is None else import_time
    data = json.loads(content, object_hook=decode_dashboards)
    if not data:
        raise DashboardImportException(_("No data in file"))
    for table in data["datasources"]:
        import_dataset(table, database_id, import_time=import_time)
    session.commit()
    for dashboard in data["dashboards"]:
        import_dashboard(dashboard, import_time=import_time)
    session.commit()


# keys to copy when converting from v0 format to v1
KEYS_TO_COPY = {
    "table_name",
    "main_dttm_col",
    "description",
    "default_endpoint",
    "offset",
    "cache_timeout",
    "sql",
    "filter_select_enabled",
    "fetch_values_predicate",
}

JSON_KEYS = {"extra", "params", "template_params"}

COLUMN_KEYS_TO_COPY = {
    "column_name",
    "verbose_name",
    "is_dttm",
    "is_active",
    "type",
    "groupby",
    "filterable",
    "expression",
    "description",
    "python_date_format",
}

METRIC_KEYS_TO_COPY = {
    "metric_name",
    "verbose_name",
    "metric_type",
    "expression",
    "description",
    "d3format",
    "warning_text",
}

CHART_KEYS_TO_COPY = {
    "slice_name",
    "viz_type",
    "cache_timeout",
}


class ImportDashboardsCommand(v1.ImportDashboardsCommand):
    """
    Import dashboard in JSON format.

    This is the original unversioned format used to export and import dashboards
    in Superset. The class simply converts the old config to the format used in
    the v1 import, and then leverages the v1 importer.
    """

    def __init__(self, contents: Dict[str, str], *args: Any, **kwargs: Any):
        super().__init__(contents, *args, **kwargs)
        self.database_id: Optional[int] = kwargs.get("database_id")

    def _convert_config(self, v0_config: Dict[str, Any]) -> Dict[str, Any]:
        configs: Dict[str, Any] = {}

        # convert datasets
        for datasource in v0_config.get("datasources", []):
            if "__SqlaTable__" in datasource:
                configs.update(
                    self._convert_dataset(datasource["__SqlaTable__"], self.database_id)
                )
        datasets = [
            (config["schema"], config["table_name"], config["uuid"])
            for file_name, config in configs.items()
            if file_name.startswith("datasets/")
        ]

        # convert charts
        for dashboard in v0_config.get("dashboards", []):
            for chart in dashboard["__Dashboard__"].get("slices", []):
                configs.update(self._convert_chart(chart["__Slice__"], datasets))

        # convert dashboards
        for dashboard in v0_config.get("dashboards", []):
            configs.update(self._convert_dashboard(dashboard["__Dashboard__"]))

        return configs

    @staticmethod
    def _convert_dataset(
        v0_config: Dict[str, Any], database_id: Optional[int]
    ) -> Dict[str, Any]:
        dataset_config = {key: v0_config[key] for key in KEYS_TO_COPY}

        # load JSON fields
        for key in JSON_KEYS:
            if v0_config.get(key):
                try:
                    dataset_config[key] = json.loads(v0_config[key])
                except json.decoder.JSONDecodeError:
                    logger.info("Unable to decode `%s` field: %s", key, v0_config[key])

        # load columns & metrics
        dataset_config["columns"] = [
            ImportDashboardsCommand._convert_column(column["__TableColumn__"])
            for column in v0_config["columns"]
        ]
        dataset_config["metrics"] = [
            ImportDashboardsCommand._convert_metric(metric["__SqlMetric__"])
            for metric in v0_config["metrics"]
        ]

        # find database
        if database_id:
            database = db.session.query(Database).filter_by(id=database_id).one()
        elif "database_name" in dataset_config["params"]:
            database = (
                db.session.query(Database)
                .filter_by(database_name=dataset_config["params"]["database_name"])
                .one()
            )
        elif v0_config.get("database_id"):
            database = (
                db.session.query(Database).filter_by(id=v0_config["database_id"]).one()
            )
        else:
            raise DatabaseNotFoundError(
                f"Could not find database for dataset `{dataset_config['table_name']}`"
            )

        # if the schema doesn't exist in the DB we assume the DB doesn't
        # support schemas instead of creating it, since the DB has already
        # been imported
        if v0_config["schema"] in database.get_all_schema_names():
            dataset_config["schema"] = v0_config["schema"]
        else:
            dataset_config["schema"] = None

        database_slug = secure_filename(database.database_name)
        dataset_slug = secure_filename(dataset_config["table_name"])
        dataset_file_name = f"datasets/{database_slug}/{dataset_slug}.yaml"

        # create DB config
        database_file_name = f"databases/{database_slug}.yaml"
        database_config = database.export_to_dict(
            recursive=False,
            include_parent_ref=False,
            include_defaults=True,
            export_uuids=True,
        )
        if database_config.get("extra"):
            try:
                database_config["extra"] = json.loads(database_config["extra"])
            except json.decoder.JSONDecodeError:
                logger.info(
                    "Unable to decode `extra` field: %s", database_config["extra"]
                )
        database_config["version"] = IMPORT_VERSION

        # add extra metadata
        dataset_config["version"] = IMPORT_VERSION
        dataset_config["uuid"] = str(uuid4())
        dataset_config["database_uuid"] = str(database.uuid)

        return {
            dataset_file_name: dataset_config,
            database_file_name: database_config,
        }

    @staticmethod
    def _convert_column(v0_config: Dict[str, Any]) -> Dict[str, Any]:
        return {key: v0_config[key] for key in COLUMN_KEYS_TO_COPY}

    @staticmethod
    def _convert_metric(v0_config: Dict[str, Any]) -> Dict[str, Any]:
        metric_config = {key: v0_config[key] for key in METRIC_KEYS_TO_COPY}

        if v0_config.get("extra"):
            try:
                metric_config["extra"] = json.loads(v0_config["extra"])
            except json.decoder.JSONDecodeError:
                logger.info("Unable to decode `extra` field: %s", v0_config["extra"])

        return metric_config

    @staticmethod
    def _convert_chart(
        v0_config: Dict[str, Any], datasets: Tuple[Optional[str], str, str]
    ) -> Dict[str, Any]:
        chart_config = {key: v0_config[key] for key in CHART_KEYS_TO_COPY}

        if v0_config.get("params"):
            try:
                chart_config["params"] = json.loads(v0_config["params"])
            except json.decoder.JSONDecodeError:
                logger.info("Unable to decode `params` field: %s", v0_config["params"])

        # find dataset associated with chart
        params = chart_config["params"]
        for schema, table_name, uuid in datasets:
            if params["datasource_name"] == table_name and (
                params["schema"] == schema or not schema
            ):
                params["schema"] = schema
                chart_config["dataset_uuid"] = uuid
                break
        else:
            raise DatasetNotFoundError(
                f"Could not find dataset for chart `{chart_config['slice_name']}`"
            )

        # add extra metadata
        chart_config["version"] = IMPORT_VERSION
        chart_config["uuid"] = str(uuid4())

        chart_slug = secure_filename(chart_config["slice_name"])
        chart_file_name = f"charts/{chart_slug}.yaml"

        return {chart_file_name: chart_config}

    @staticmethod
    def _convert_dashboard(v0_config: Dict[str, Any]) -> Dict[str, Any]:
        pass

    def validate(self) -> None:
        # ensure all files are JSON
        for file_name, content in self.contents.items():
            try:
                config = json.loads(content)
            except ValueError:
                logger.exception("Invalid JSON file")
                raise IncorrectVersionError(f"{file_name} is not a valid JSON file")

            if "dashboards" not in config:
                raise IncorrectVersionError(f"{file_name} has no valid keys")

            v1_config = self._convert_config(config)
            self._configs.update(v1_config)
