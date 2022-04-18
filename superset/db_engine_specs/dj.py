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

import re
from typing import Any, List, Optional, TYPE_CHECKING

import requests
from sqlalchemy.engine.reflection import Inspector

from superset.db_engine_specs.base import BaseEngineSpec, MetricType

if TYPE_CHECKING:
    from superset.models.core import Database


SELECT_STAR_MESSAGE = (
    "DJ does not support data preview, since the `metrics` table is a virtual table "
    "representing the whole metric repository. An administrator should configure the "
    "DJ database with the `disable_data_preview` attribute set to `true` in the `extra` "
    "field."
)


class DJEngineSpec(BaseEngineSpec):  # pylint: disable=abstract-method
    """
    Engine spec for the DataJunction metric repository

    See https://github.com/DataJunction/datajunction for more information.
    """

    engine = "dj"
    engine_name = "DJ"

    sqlalchemy_uri_placeholder = "dj://host:port/database_id"

    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "DATE_TRUNC('second', {col})",
        "PT1M": "DATE_TRUNC('minute', {col})",
        "PT1H": "DATE_TRUNC('hour', {col})",
        "P1D": "DATE_TRUNC('day', {col})",
        "P1W": "DATE_TRUNC('week', {col})",
        "P1M": "DATE_TRUNC('month', {col})",
        "P3M": "DATE_TRUNC('quarter', {col})",
        "P1Y": "DATE_TRUNC('year', {col})",
    }

    @classmethod
    def select_star(  # pylint: disable=unused-argument
        cls,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """
        Return a ``SELECT *`` query.

        Since DJ doesn't have tables per se, a ``SELECT *`` query doesn't make sense.
        """
        message = SELECT_STAR_MESSAGE.replace("'", "''")
        return f"SELECT '{message}' AS message"

    @classmethod
    def get_metrics(
        cls,
        database: "Database",
        inspector: Inspector,
        table_name: str,
        schema: Optional[str],
    ) -> List[MetricType]:
        """
        Get all metrics from a given schema and table.
        """
        engine = database.get_sqla_engine()
        base_url = engine.connect().connection.base_url

        response = requests.get(base_url / "metrics/")
        payload = response.json()
        return [
            {
                "metric_name": metric["name"],
                "expression": f'"{metric["name"]}"',
                "description": metric["description"],
            }
            for metric in payload
        ]

    @classmethod
    def execute(
        cls,
        cursor: Any,
        query: str,
        **kwargs: Any,
    ) -> None:
        """
        Quote ``__timestamp`` and other identifiers starting with an underscore.
        """
        query = re.sub(r" AS (_.*)(\b|$)", r' AS "\1"', query)

        return super().execute(cursor, query, **kwargs)
