from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from scripts.blondon_communities.blondon_communities_list_stations import SupabaseWriter


@dataclass
class FakeResponse:
    data: Any


class FakeQuery:
    def __init__(self, core: "FakeCore", table: str) -> None:
        self.core = core
        self.table = table
        self.operation = ""
        self.fields: Any = None

    def select(self, fields: str) -> "FakeQuery":
        self.operation = "select"
        self.fields = fields
        return self

    def upsert(self, payload: Any, **_kwargs: Any) -> "FakeQuery":
        self.operation = "upsert"
        self.fields = payload
        return self

    def eq(self, *_args: Any) -> "FakeQuery":
        return self

    def limit(self, *_args: Any) -> "FakeQuery":
        return self

    def execute(self) -> FakeResponse:
        self.core.calls.append((self.table, self.operation, self.fields))
        return self.core.responses.pop(0)


class FakeCore:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, Any]] = []

    def table(self, table: str) -> FakeQuery:
        return FakeQuery(self, table)


def make_writer(responses: list[FakeResponse]) -> tuple[SupabaseWriter, FakeCore]:
    writer = SupabaseWriter.__new__(SupabaseWriter)
    core = FakeCore(responses)
    writer.core = core
    return writer, core


def test_existing_connector_reuses_initial_lookup_id_without_second_lookup() -> None:
    writer, core = make_writer([
        FakeResponse([{"id": 41, "poll_enabled": True}]),
        FakeResponse([{"id": 41}]),
    ])

    assert writer.upsert_connector() == 41
    assert [(table, operation) for table, operation, _fields in core.calls] == [
        ("connectors", "select"),
        ("connectors", "upsert"),
    ]


def test_new_connector_uses_id_returned_by_idempotent_upsert() -> None:
    writer, core = make_writer([
        FakeResponse([]),
        FakeResponse([{"id": 42}]),
    ])

    assert writer.upsert_connector() == 42
    assert [(table, operation) for table, operation, _fields in core.calls] == [
        ("connectors", "select"),
        ("connectors", "upsert"),
    ]


def test_connector_id_lookup_is_only_used_when_upsert_response_has_no_id() -> None:
    writer, core = make_writer([
        FakeResponse([]),
        FakeResponse([]),
        FakeResponse([{"id": 43}]),
    ])

    assert writer.upsert_connector() == 43
    assert [(table, operation, fields) for table, operation, fields in core.calls] == [
        ("connectors", "select", "id,poll_enabled"),
        ("connectors", "upsert", core.calls[1][2]),
        ("connectors", "select", "id"),
    ]
