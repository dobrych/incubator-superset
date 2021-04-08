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
import datetime
import operator
import urllib.parse
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

from flask import g
from flask_login import current_user
from shillelagh.adapters.base import Adapter
from shillelagh.backends.apsw.dialect import APSWDialect
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import (
    Blob,
    Boolean,
    Date,
    DateTime,
    Field,
    Float,
    Integer,
    Order,
    String,
    Time,
)
from shillelagh.filters import Equal, Filter, Range
from shillelagh.types import RequestedOrder, Row
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.pool.base import _ConnectionFairy
from sqlalchemy.sql import Select, select

from superset import db, security_manager, sql_parse
from superset.db_engine_specs.sqlite import SqliteEngineSpec


class SupersetEngineSpec(SqliteEngineSpec):
    """
    Internal engine for Superset

    This DB engine spec is a meta-database. It uses the shillelagh library
    to build a DB that can operate across different Superset databases.
    """

    engine = "superset"
    engine_name = "Superset"


class SupersetAPSWDialect(APSWDialect):

    """
    A SQLAlchemy dialect for an internal Superset engine.

    This dialect allows query to be executed across different Superset
    databases. For example, to read data from the `birth_names` table in the
    `examples` databases:

        >>> engine = create_engine('superset://')
        >>> conn = engine.connect()
        >>> results = conn.execute('SELECT * FROM "superset.examples.birth_names"')

    Queries can also join data across different Superset databases.

    The dialect is built in top of the shillelagh library, leveraging SQLite to
    create virtual tables on-the-fly proxying Superset tables. The
    `SupersetShillelaghAdapter` adapter is responsible for returning data when a
    Superset table is accessed.
    """

    name = "superset"

    def create_connect_args(self, url: URL) -> Tuple[Tuple[()], Dict[str, Any]]:
        return (
            (),
            {
                "path": ":memory:",
                "adapters": ["superset"],
                "adapter_args": {},
                "safe": True,
                "isolation_level": self.isolation_level,
            },
        )

    def get_schema_names(
        self, connection: _ConnectionFairy, **kwargs: Any
    ) -> List[str]:
        return []


class SupersetShillelaghAdapter(Adapter):

    """
    A shillelagh adapter for Superset tables.

    Shillelagh adapters are responsible for fetching data from a given resource,
    allowing it to be represented as a virtual table in SQLite.
    """

    safe = True

    type_map: Dict[Any, Type[Field]] = {
        bool: Boolean,
        float: Float,
        int: Integer,
        str: String,
        datetime.date: Date,
        datetime.datetime: DateTime,
        datetime.time: Time,
    }

    @staticmethod
    def supports(uri: str) -> bool:
        # An URL for a table has the format superset.database[.catalog][.schema].table,
        # eg, superset.examples.birth_names
        parsed = urllib.parse.urlparse(uri)
        parts = parsed.path.split(".")
        return 3 <= len(parts) <= 5 and parts[0] == "superset"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, Optional[str], Optional[str], str]:
        parsed = urllib.parse.urlparse(uri)
        parts = parsed.path.split(".")
        if len(parts) == 3:
            return parts[1], None, None, parts[2]
        if len(parts) == 4:
            return parts[1], None, parts[2], parts[3]
        return tuple(parts[1:])  # type: ignore

    def __init__(
        self, database: str, catalog: Optional[str], schema: Optional[str], table: str,
    ):
        self.database = database
        self.catalog = catalog
        self.schema = schema
        self.table = table

        self._set_columns()

    @classmethod
    def get_field(cls, python_type: Any) -> Field:
        class_ = cls.type_map.get(python_type, Blob)
        return class_(filters=[Equal, Range], order=Order.ANY, exact=True)

    def _set_columns(self) -> None:
        from superset.models.core import Database

        database = (
            db.session.query(Database).filter_by(database_name=self.database).one()
        )

        # verify permissions
        g.user = current_user
        table = sql_parse.Table(self.table, self.schema, self.catalog)
        security_manager.raise_for_access(database=database, table=table)

        # fetch column names and types
        self.engine = database.get_sqla_engine()
        metadata = MetaData()
        self._table = Table(
            self.table, metadata, autoload=True, autoload_with=self.engine,
        )

        self.columns = {
            column.name: self.get_field(column.type.python_type)
            for column in self._table.c
        }

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def _build_sql(
        self, bounds: Dict[str, Filter], order: List[Tuple[str, RequestedOrder]]
    ) -> Select:
        query = select([self._table])

        for column_name, filter_ in bounds.items():
            column = self._table.c[column_name]
            if isinstance(filter_, Equal):
                query = query.where(column == filter_.value)
            elif isinstance(filter_, Range):
                if filter_.start is not None:
                    op = operator.ge if filter_.include_start else operator.gt
                    query = query.where(op(column, filter_.start))
                if filter_.end is not None:
                    op = operator.le if filter_.include_end else operator.ge
                    query = query.where(op(column, filter_.end))
            else:
                raise ProgrammingError(f"Invalid filter: {filter_}")

        for column_name, requested_order in order:
            column = self._table.c[column_name]
            if requested_order == Order.DESCENDING:
                column = column.desc()
            query = query.order_by(column)

        return query

    def get_data(
        self, bounds: Dict[str, Filter], order: List[Tuple[str, RequestedOrder]]
    ) -> Iterator[Row]:
        query = self._build_sql(bounds, order)

        connection = self.engine.connect()
        rows = connection.execute(query)
        for i, row in enumerate(rows):
            data = dict(zip(self.columns, row))
            data["rowid"] = i
            yield data
