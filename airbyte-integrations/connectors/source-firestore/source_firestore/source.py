#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.utils import casing
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode, SyncMode
from google.oauth2 import service_account
import google.auth.transport.requests
import requests
import json
import re

class Helpers(object):
    url_base = "https://firestore.googleapis.com/v1/"
    page_size = 10000

    @staticmethod
    def get_collection_path(project_id: str, collection_name: str) -> str:
        return f"projects/{project_id}/databases/(default)/documents:runQuery"

    @staticmethod
    def get_project_url(project_id: str) -> str:
        return f"{Helpers.url_base}projects/{project_id}"

    @staticmethod
    def get_collections_list_url(project_id: str) -> str:
        return f"{Helpers.get_project_url(project_id)}/databases/(default)/documents:listCollectionIds"

    @staticmethod
    def parse_date(date: str) -> datetime:
        return datetime.fromisoformat(date.replace("Z", "+00:00"))

# Basic full refresh stream
class FirestoreStream(HttpStream, ABC):
    documents_read: int = 0
    _cursor_value: Optional[datetime]
    _attempts_same_date_value: int = 0
    cursor_field: Union[str, List[str], None] = []
    @property
    def cursor_key(self):
        if isinstance(self.cursor_field, list):
            if (len(self.cursor_field) > 0):
                return self.cursor_field[0]
            return None
        return self.cursor_field
    attempts_same_date_key: str = "attempts_same_date"
    max_attempts_same_date: int = 2

    url_base: str = Helpers.url_base
    _primary_key: str = "__name__"
    page_size: int = Helpers.page_size
    http_method: str = "POST"
    collection_name: str
    first_document: Union[dict[str, Any], None]

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value: str) -> None:
        if not isinstance(value, property):
            self._primary_key = value

    @property
    def name(self):
        return casing.camel_to_snake(self.collection_name)

    def __init__(self, authenticator: TokenAuthenticator, collection_name: str):
        super().__init__(
            authenticator=authenticator,
        )
        self._cursor_value = None
        self.collection_name = collection_name

        self.first_document = self.get_first_document(authenticator=authenticator)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        documents = list(self.parse_response(response))

        if len(documents) == 0:
            return None
        if self.cursor_key is None:
            return None
        if self.documents_read > 500000:
            return None

        doc = documents[-1]

        first_name = documents[0]["name"]
        self.logger.info(f"Stream {self.name}: First page token: {first_name}")

        if self.cursor_key == "__name__":
            r = { "name": doc["name"], "__name__": doc["name"] }
        else:
            r = { "name": doc["name"], self.cursor_key: doc[self.cursor_key] }

        self.logger.info(f"Stream {self.name}: Next page token: {r}")

        return r

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = {}, next_page_token: Mapping[str, Any] = {}
    ) -> MutableMapping[str, Any]:
        return {}

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = {},
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Mapping]:
        page_size: int = stream_state.get("page_size", self.page_size)
        attempts_same_date: int = stream_state.get(self.attempts_same_date_key) or 0
        timestamp_state: Optional[str] = next_page_token.get(self.cursor_key) if next_page_token and self.cursor_key and self.cursor_key != "__name__" else None
        timestamp_value = Helpers.parse_date(timestamp_state) if timestamp_state else None
        if timestamp_state is None:
            timestamp_state = stream_state.get(self.cursor_key)  if self.cursor_key else None
            timestamp_value = Helpers.parse_date(timestamp_state) - timedelta(minutes=1) if timestamp_state else None
        next_page_token_name = next_page_token.get("name") if next_page_token else None

        self.logger.info(f"Stream {self.name}: Requesting body JSON for collection {self.collection_name} with cursor {self.cursor_key} (value: {timestamp_value}), name: {next_page_token_name} and page_size: {page_size}")

        if attempts_same_date >= self.max_attempts_same_date:
            start_at = timestamp_value
            before = False
        else:
            start_at = timestamp_value if timestamp_value else None
            before = True

        order_by = []
        start_at_values = []

        if self.cursor_key and self.cursor_key != "__name__":
            order_by.append({ "field": { "fieldPath": self.cursor_key }, "direction": "ASCENDING" })
            start_at_values.append({ "timestampValue": start_at.isoformat() if start_at else datetime.fromtimestamp(0, tz=timezone.utc).isoformat() })
        if next_page_token_name:
            order_by.append({ "field": { "fieldPath": "__name__" }, "direction": "ASCENDING" })
            start_at_values.append({ "referenceValue": next_page_token_name })
            before = False

        self.logger.info(f"Stream {self.name}: startAt values: {start_at_values}, order by: {order_by}")

        return {
            "structuredQuery": {
                "from": [{"collectionId": self.collection_name, "allDescendants": True}],
                "offset": 0,
                "limit": page_size,
                "orderBy": order_by if len(order_by) > 0 else None,
                "startAt": {"values": start_at_values, "before": before } if len(start_at_values) > 0 else None,
            }
        }
    
    def check_availability(self, logger, source = None) -> Tuple[bool, Optional[str]]:
        return True, None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        results = []
        for entry in data:
            if "document" in entry:
                document = entry["document"]
                if "fields" in document:
                    result = {
                        "name": document["name"],
                        "json_data": self.parse_json_data(document["fields"]),
                    } | self.parse_path_to_ids_dict(document["name"])
                    
                    if self.cursor_key and self.cursor_key in document["fields"]:
                        result[self.cursor_key] = document["fields"][self.cursor_key]

                    results.append(result)

        return iter(results)

    def parse_json_data(self, data: Any) -> Any:
        for entry in data:
            if "integerValue" in data[entry]:
                data[entry] = data[entry]["integerValue"]
            elif "doubleValue" in data[entry]:
                data[entry] = data[entry]["doubleValue"]
            elif "booleanValue" in data[entry]:
                data[entry] = data[entry]["booleanValue"]
            elif "timestampValue" in data[entry]:
                data[entry] = data[entry]["timestampValue"]
            elif "stringValue" in data[entry]:
                data[entry] = data[entry]["stringValue"]
            elif "bytesValue" in data[entry]:
                data[entry] = data[entry]["bytesValue"]
            elif "referenceValue" in data[entry]:
                data[entry] = data[entry]["referenceValue"]
            elif "geoPointValue" in data[entry]:
                data[entry] = data[entry]["geoPointValue"]
            elif "nullValue" in data[entry]:
                data[entry] = data[entry]["nullValue"]
            elif "arrayValue" in data[entry]:
                arr = []
                if "values" in data[entry]["arrayValue"]:
                    for item in data[entry]["arrayValue"]["values"]:
                        arr.append(self.parse_json_data({ "value": item })["value"])
                data[entry] = arr
            elif "mapValue" in data[entry]:
                data[entry] = self.parse_json_data(data[entry]["mapValue"]["fields"])
        return data
    
    def parse_path_to_ids_dict(self, path: str) -> dict[str, str]:
        pattern = r'.*?' + re.escape("databases/(default)/documents/")
        path = re.sub(pattern, '', path, count=1)
        parts = path.strip('/').split('/')
        result = {}
        
        for i in range(0, len(parts) - 2, 2):
            if i + 1 < len(parts):
                prefix = parts[i]
                if prefix.endswith('s'):
                    key = f"{prefix[:-1]}_id"
                else:
                    key = f"{prefix}_id"
                value = parts[i + 1]
                result[key] = value
        if len(parts) >= 2:
            last_id = parts[-1]
            result['id'] = last_id
        
        return result

    def get_first_document(self, authenticator: TokenAuthenticator) -> Union[dict[str, Any], None]:
        url = f"{Helpers.url_base}{Helpers.get_collection_path(self.project_id, self.collection_name)}"
        headers = authenticator.get_auth_header()
        data = json.dumps({
            "structuredQuery": {
                "from": [{"collectionId": self.collection_name, "allDescendants": True}],
                "offset": 0,
                "limit": 1,
                "orderBy": [{ "field": { "fieldPath": "__name__" }, "direction": "ASCENDING" }]
            }
        })
        request = requests.post(url, headers=headers, data=data)
        records = list(request.json())

        return records[0] if len(records) > 0 else None

    def get_json_schema(self) -> Mapping[str, Any]:
        additional_ids: dict[str, Any] = {}
        if self.first_document is not None and "document" in self.first_document:
            ids = self.parse_path_to_ids_dict(self.first_document["document"]["name"])
            for key in ids:
                additional_ids[key] = { "type": "string" }
        result = {
            "type": "object",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "additionalProperties": True,
            "required": ["name"],
            "properties": {
                "name": { "type": "string" },
                "json_data": { "type": "object" },
            } | additional_ids,
        }
        if self.cursor_key:
            result["properties"][self.cursor_key] = { "type": ["null", "string"] }
        return result


class IncrementalFirestoreStream(FirestoreStream, IncrementalMixin):
    start_date: Optional[datetime]

    def __init__(self, authenticator: TokenAuthenticator, collection_name: str, cursor_field_possibilities: List[str] = []):
        super().__init__(authenticator=authenticator, collection_name=collection_name)
        self.cursor_field = self.guess_cursor_field(cursor_field_possibilities)
        self.logger.info(f"Stream {self.name}: Guessed cursor field {self.cursor_field})")

    def guess_cursor_field(self, cursor_field_possibilities: List[str]) -> Union[str, None]:
        self.logger.info(f"Stream {self.name}: Guessing from 1 record")
        if self.first_document is None or "document" not in self.first_document:
            return None
        # try to guess default field from first record
        for cursor_field in cursor_field_possibilities:
            if cursor_field in self.first_document["document"]["fields"]:
                return cursor_field
        return "__name__"

    @property
    def state(self) -> dict[str, Any]:
        return { self.cursor_key: self._cursor_value.isoformat(), self.attempts_same_date_key: self._attempts_same_date_value + 1 } if self._cursor_value and self.cursor_key else {}

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        new_cursor_value = value.get(self.cursor_key, self.start_date) if self.cursor_key else None
        self._cursor_value = Helpers.parse_date(new_cursor_value) if isinstance(new_cursor_value, str) else new_cursor_value
        self._attempts_same_date_value = value.get(self.attempts_same_date_key, 0)
        self.logger.info(f"Setting state: {self.cursor_key} = {self._cursor_value} (parsed from {new_cursor_value})")

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = {}, next_page_token: Mapping[str, Any] = {}
    ) -> MutableMapping[str, Any]:
        return super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = [],
        stream_slice: Mapping[str, Any] = {},
        stream_state: Mapping[str, Any] = {},
    ) -> Iterable[Mapping[str, Any]]:
        self.logger.info(f"Stream {self.name}: Reading in {sync_mode} (cursor field {cursor_field}). Current cursor value: {self._cursor_value}")
        for record in super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        ):
            yield record
            if sync_mode == SyncMode.incremental:
                self.documents_read += 1
                record_date = Helpers.parse_date(record[self.cursor_key]) if self.cursor_key and self.cursor_key != "__name__" else None
                if record_date:
                    new_cursor_value = max(record_date, self._cursor_value) if self._cursor_value else record_date
                    self._attempts_same_date_value = 0 if new_cursor_value != self._cursor_value else self._attempts_same_date_value
                    self._cursor_value = new_cursor_value

class Collection(IncrementalFirestoreStream):
    project_id: str
    collection_name: str

    def __init__(self, authenticator: TokenAuthenticator, collection_name: str, config: Mapping[str, Any]):
        self.project_id = config["project_id"]
        self.collection_name = collection_name
        self.start_date = Helpers.parse_date(config["start_date"]) if "start_date" in config else None
        super().__init__(authenticator, collection_name=collection_name, cursor_field_possibilities=config["cursor_field_possibilities"])

    def path(
        self, stream_state: Mapping[str, Any] = {}, stream_slice: Mapping[str, Any] = {}, next_page_token: Mapping[str, Any] = {}
    ) -> str:
        return Helpers.get_collection_path(self.project_id, self.collection_name)

# Source
class SourceFirestore(AbstractSource):
    def get_auth(self, config: Mapping[str, Any]) -> TokenAuthenticator:
        print("Get auth")
        scopes = ['https://www.googleapis.com/auth/datastore']
        credentials = service_account.Credentials.from_service_account_info(json.loads(config["google_application_credentials"]), scopes=scopes)        
        credentials.refresh(google.auth.transport.requests.Request())
        token: Any = credentials.token
        return TokenAuthenticator(token=token)

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        print("Check connection")
        auth = self.get_auth(config=config)
        project_id = config["project_id"]
        url = Helpers.get_collections_list_url(project_id)
        try:
            response = requests.get(url, headers=auth.get_auth_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            return False, str(e)
        return True, None

    def discover_collections(self, project_id: str, auth: TokenAuthenticator) -> List[str]:
        print("Discovering collections")
        url = Helpers.get_collections_list_url(project_id)
        response = requests.post(url, headers=auth.get_auth_header())
        response.raise_for_status()
        json = response.json()
        return json.get("collectionIds", [])

    def streams(self, config: Mapping[str, Any]):
        print("Initializing streams")
        auth = self.get_auth(config=config)
        project_id = config["project_id"]
        collection_groups = config["collection_groups"] if "collection_groups" in config else []
        collections = self.discover_collections(project_id, auth)
        return map(lambda collection_name : Collection(auth, collection_name, config), collections + collection_groups)
