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
from datetime import datetime
from typing import Any, Dict, Optional

import requests

from superset.integrations.data_portal.base import TableMetadataFetcher

stat_map = {
    "min": "min",
    "max": "max",
    "average": "mean",
    "num nulls": "nulls",
    "distinct values": "cardinality",
}


class Amundsen(TableMetadataFetcher):  # pylint: disable=too-few-public-methods
    """
    Fetch metadata from Amundsen (https://www.amundsen.io/).

    To use, enable the metadata extractor in ``superset_config.py``:

        TABLE_METADATA_FETCHER = Amundsen("http://localhost:5000/", {1: "hive://gold."})

    """

    def __init__(self, baseurl: str, database_map: Dict[int, str]) -> None:
        self.baseurl = baseurl.rstrip("/")
        self.database_map = database_map

    def get_metadata(
        self,
        database_id: int,
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
    ) -> Dict[str, Any]:
        database_prefix = self.database_map[database_id]
        url = f"{self.baseurl}/api/metadata/v0/table?key={database_prefix}{schema}/{table}"

        response = requests.get(url)
        payload = response.json()

        table_metadata = {}
        description = payload["tableData"].get("description")
        if description:
            table_metadata["description"] = description
        last_updated = payload["tableData"].get("last_updated_timestamp")
        if last_updated:
            table_metadata["last updated"] = datetime.fromtimestamp(
                last_updated
            ).isoformat()

        column_metadata: Dict[str, Dict[str, Any]] = {}
        for column in payload["tableData"]["columns"]:
            name = column["name"]
            column_metadata[name] = {
                "description": column["description"],
            }
            for stat in column["stats"]:
                type_ = stat["stat_type"]
                value = stat["stat_val"]
                standard_name = stat_map.get(type_, type_)
                column_metadata[name][standard_name] = json.loads(value)

        return {"table": table_metadata, "columns": column_metadata}
