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

from typing import Any, Dict, List, Optional, Tuple

import yaml
from marshmallow import fields, Schema, validate
from marshmallow.exceptions import ValidationError
from sqlalchemy.orm import Session

from superset import db
from superset.commands.base import BaseCommand
from superset.commands.exceptions import CommandInvalidError
from superset.commands.importers.exceptions import IncorrectVersionError
from superset.models.helpers import ImportExportMixin

METADATA_FILE_NAME = "metadata.yaml"
IMPORT_VERSION = "1.0.0"


def load_yaml(
    file_name: str, content: str
) -> Tuple[Optional[Dict[str, Any]], Optional[ValidationError]]:
    """Try to load a YAML file"""
    try:
        config = yaml.safe_load(content)
        exc = None
    except yaml.parser.ParserError:
        config = None
        exc = ValidationError({file_name: "Not a valida YAML file"})

    return config, exc


class MetadataSchema(Schema):
    version = fields.String(required=True, validate=validate.Equal(IMPORT_VERSION))
    type = fields.String(required=True)
    timestamp = fields.DateTime()


class ImportModelsCommand(BaseCommand):

    model = ImportExportMixin
    schema = Schema
    prefix = ""

    # pylint: disable=unused-argument
    def __init__(self, contents: Dict[str, str], *args: Any, **kwargs: Any):
        self.contents = contents
        self._configs: Dict[str, Any] = {}

    @staticmethod
    def import_(
        session: Session,
        config: Dict[str, Any],
        overwrite: bool = False,
    ) -> ImportExportMixin:
        raise NotImplementedError("Subclasss MUST implement import_")

    def import_bundle(self, session: Session) -> None:
        # a generic function to import all assets of a given type;
        # subclasses will want to overload this to also import related
        # assets, eg, importing charts during a dashboard import
        for file_name, config in self._configs.items():
            if file_name.startswith(self.prefix):
                self.import_(session, config)

    def run(self) -> None:
        self.validate()

        # rollback to prevent partial imports
        try:
            self.import_bundle(db.session)
            db.session.commit()
        except Exception as exc:
            db.session.rollback()
            raise exc

    @classmethod
    def validate_schema(cls, config: Dict[str, Any]) -> None:
        cls.schema().load(config)

    def validate(self) -> None:
        """
        Validate that the import can be done and matches the schema.

        When validating the import we raise IncorrectVersionError when the
        file is not supported (eg, a v0 import for the v1 importer), which
        allows the dispatcher to try a different command.

        If the file is supported we collect any validation errors, and if
        there are any we raise CommandInvalidError with all the collected
        messages.
        """
        exceptions: List[ValidationError] = []

        metadata: Optional[Dict[str, Any]] = None
        if METADATA_FILE_NAME not in self.contents:
            raise IncorrectVersionError(f"Missing {METADATA_FILE_NAME}")

        content = self.contents[METADATA_FILE_NAME]
        metadata, exc = load_yaml(METADATA_FILE_NAME, content)
        if exc:
            raise IncorrectVersionError("Not a YAML file")

        try:
            MetadataSchema().load(metadata)
        except ValidationError as exc:
            # if it's a version error raise IncorrectVersionError so that
            # the dispatcher can try the next version; otherwise store
            # the validation message
            if "version" in exc.messages:
                raise IncorrectVersionError(exc.messages["version"])
            exc.messages = {METADATA_FILE_NAME: exc.messages}
            exceptions.append(exc)

        # validate that the type in METADATA_FILE_NAME matches the
        # type of the model being imported; this prevents exporting
        # a chart and importing as a database, eg, to prevent
        # confusion or error
        type_validator = validate.Equal(self.model.__name__)
        try:
            type_validator(metadata["type"])
        except ValidationError as exc:
            exc.messages = {METADATA_FILE_NAME: {"type": exc.messages}}
            exceptions.append(exc)

        for file_name, content in self.contents.items():
            config, exc = load_yaml(file_name, content)
            if exc:
                exceptions.append(exc)

            if config and file_name.startswith(self.prefix):
                try:
                    self.validate_schema(config)
                except ValidationError as exc:
                    exc.messages = {file_name: exc.messages}
            self._configs[file_name] = config

        if exceptions:
            exc = CommandInvalidError("Error importing model")
            exc.add_list(exceptions)
            raise exc
