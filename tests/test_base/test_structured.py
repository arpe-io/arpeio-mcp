"""Tests for the shared structured-output helpers."""

import jsonschema
from mcp.types import TextContent

from src.base.structured import make_output_schema, respond


class TestMakeOutputSchema:
    def test_always_requires_status(self):
        schema = make_output_schema({})
        assert schema["required"] == ["status"]
        assert schema["properties"]["status"]["enum"] == ["ok", "error"]

    def test_is_permissive(self):
        """Extra fields must be allowed so every return path validates."""
        schema = make_output_schema({})
        assert schema["additionalProperties"] is True

    def test_merges_extra_properties(self):
        schema = make_output_schema({"command": {"type": "array"}})
        assert "command" in schema["properties"]
        assert "status" in schema["properties"]

    def test_both_ok_and_error_payloads_validate(self):
        schema = make_output_schema({"command": {"type": "array"}})
        # success payload
        jsonschema.validate({"status": "ok", "command": ["a", "b"]}, schema)
        # error payload with completely different shape still validates
        jsonschema.validate({"status": "error", "errors": [{"field": "x"}]}, schema)


class TestRespond:
    def test_returns_tuple_of_content_and_structured(self):
        content, structured = respond("hello", {"status": "ok"})
        assert isinstance(content, list)
        assert isinstance(content[0], TextContent)
        assert content[0].text == "hello"
        assert structured == {"status": "ok"}

    def test_joins_list_of_lines(self):
        content, _ = respond(["a", "b", "c"], {"status": "ok"})
        assert content[0].text == "a\nb\nc"

    def test_structured_matches_a_schema_built_for_it(self):
        schema = make_output_schema({"hits": {"type": "integer"}})
        _, structured = respond("x", {"status": "ok", "hits": 3})
        jsonschema.validate(structured, schema)
