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
# pylint: disable=too-few-public-methods
import fnmatch
import logging
from collections import defaultdict
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional

import simplejson as json
from celery.utils.log import get_task_logger
from sqlalchemy import distinct, func, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.sql import and_, select
from typing_extensions import TypedDict

from superset import db
from superset.extensions import celery_app
from superset.models.core import Database
from superset.utils.core import get_main_database, json_iso_dttm_ser

logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)

# SELECT statements in Postgres can have at most 1664 entries
MAX_NUMBER_OF_COLUMNS = 1664


aggregation_functions = {
    "min": (func.min, {int, float, str, datetime, date, time}),
    "max": (func.max, {int, float, str, datetime, date, time}),
    "mean": (func.avg, {int, float}),
    "stddev": (func.stddev_samp, {int, float}),
    "cardinality": (
        lambda col: func.count(distinct(col)),
        {int, float, str, datetime, date, time},
    ),
    "nulls": (
        lambda col: func.count() - func.count(col),
        {int, float, str, datetime, date, time, bool},
    ),
}


class DatabaseConfig(TypedDict):
    id: int
    path: str
    statistics: List[str]


def get_tables(database: Database, path: str) -> List[str]:
    """
    List all the tables in a database matching the path pattern.
    """
    database.allow_multi_schema_metadata_fetch = True

    all_tables = [
        f"{table.schema}/{table.table}".lstrip("/")
        for table in database.get_all_table_names_in_database()
    ]
    matching_tables = [table for table in all_tables if fnmatch.fnmatch(table, path)]

    return matching_tables


def get_metadata(
    engine: Engine, metadata: MetaData, table_path: str, statistics: List[str]
) -> Dict[str, Any]:
    """
    Index metadata and statistics about a table.
    """
    schema: Optional[str]
    if "/" in table_path:
        schema, name = table_path.split("/", 1)
    else:
        schema, name = None, table_path

    table = Table(name, metadata, schema=schema, autoload=True, autoload_with=engine,)

    aggregations = []
    for column in table.columns:
        for statistic in statistics:
            if statistic in aggregation_functions:
                aggregation, supported_types = aggregation_functions[statistic]
                if column.type.python_type in supported_types:
                    aggregations.append(aggregation(column))

    connection = engine.connect()
    values = []
    while aggregations:
        entries, aggregations = (
            aggregations[:MAX_NUMBER_OF_COLUMNS],
            aggregations[MAX_NUMBER_OF_COLUMNS:],
        )
        query = select(entries).select_from(table)
        results = connection.execute(query)
        values.extend(results.fetchone())

    # column metadata
    it = iter(values)
    columns: Dict[str, Dict[str, Any]] = defaultdict(dict)
    for column in table.columns:
        for statistic in statistics:
            if statistic in aggregation_functions:
                supported_types = aggregation_functions[statistic][1]
                if column.type.python_type in supported_types:
                    columns[column.name][statistic] = next(it)

    metadata = {"table": {}, "columns": columns}

    return metadata


@celery_app.task(name="collect_metadata")
def collect_metadata(databases: List[DatabaseConfig]) -> None:
    """
    Collect metadata and statistics from tables.
    """
    metadata = MetaData()
    main_database = get_main_database()
    main_engine = main_database.get_sqla_engine()
    target_table = Table(
        "table_metadata", metadata, autoload=True, autoload_with=main_engine
    )

    for config in databases:
        database = db.session.query(Database).filter_by(id=config["id"]).scalar()
        engine = database.get_sqla_engine()

        for table in get_tables(database, config["path"]):
            logger.info("Indexing %s", table)

            table_metadata = get_metadata(engine, metadata, table, config["statistics"])

            schema: Optional[str]
            if "/" in table:
                schema, name = table.split("/", 1)
            else:
                schema, name = None, table

            connection = main_engine.connect()
            transaction = connection.begin()
            try:
                delete = target_table.delete().where(
                    and_(
                        target_table.c.database_id == config["id"],
                        target_table.c.catalog.is_(None),
                        target_table.c.schema == schema,
                        target_table.c.table_name == name,
                    )
                )
                connection.execute(delete)

                insert = target_table.insert().values(
                    database_id=config["id"],
                    catalog=None,
                    schema=schema,
                    table_name=name,
                    metadata=json.dumps(table_metadata, default=json_iso_dttm_ser),
                )
                connection.execute(insert)
                transaction.commit()
            except Exception as ex:
                logger.warning("Error upserting metadata: %s", ex)
                transaction.rollback()
