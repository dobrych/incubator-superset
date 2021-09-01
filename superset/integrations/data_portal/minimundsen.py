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
from typing import Any, Dict, Optional

from sqlalchemy import MetaData, Table
from sqlalchemy.sql import and_, select

from superset.integrations.data_portal.base import TableMetadataFetcher
from superset.utils.core import get_main_database


class MiniMundsen(TableMetadataFetcher):  # pylint: disable=too-few-public-methods
    """
    A simple metadata extractor.

    To use, configure a celery task like this:

        CELERY_IMPORTS = (
            "superset.tasks.collect_metadata",
            ...
        )
        CELERYBEAT_SCHEDULE = {
            "collect_metadata": {
                "task": "collect_metadata",
                "schedule": crontab(minute="0", hour="*"),
                "kwargs": {
                    "databases": [
                        {
                            "id": 1,
                            "path": "public/*",
                            "statistics": [
                                "min",
                                "max",
                                "mean",
                                "stddev",
                                "cardinality",
                                "nulls",
                            ],
                        },
                    ],
                },
            },
            ...
        }

    The configuration above will index database 1, computing all the available
    statistics (min, max, etc.) for tables in the ``public`` schema.

    Then, enable the metadata extractor in ``superset_config.py``:

        TABLE_METADATA_FETCHER = MiniMundsen()

    """

    def get_metadata(
        self,
        database_id: int,
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
    ) -> Dict[str, Dict[str, Any]]:
        metadata = MetaData()

        database = get_main_database()
        engine = database.get_sqla_engine()

        metadata_table = Table(
            "table_metadata", metadata, autoload=True, autoload_with=engine,
        )

        query = select([metadata_table.c.metadata]).where(
            and_(
                metadata_table.c.database_id == database_id,
                metadata_table.c.catalog == catalog,
                metadata_table.c.schema == schema,
                metadata_table.c.table_name == table,
            )
        )

        connection = engine.connect()
        results = connection.execute(query)
        table_metadata = results.fetchone()[0]

        return json.loads(table_metadata)
