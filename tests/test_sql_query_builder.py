"""Tests for appfx.cosmosdb.sql.repository query helpers."""

# NOTE: We intentionally omit `from __future__ import annotations` here because
# the production code's _get_field_type_info() inspects __annotations__ at runtime
# and requires actual types, not PEP 563 deferred strings.

from unittest.mock import patch

import pytest

from appfx.cosmosdb._base.repository_base import SortDirection, SortField
from appfx.cosmosdb.sql.model import EntityBase as SqlEntityBase
from appfx.cosmosdb.sql.model import RootEntityBase
from appfx.cosmosdb.sql.repository import RepositoryBase

# ── Test Entities ───────────────────────────────────────────────────────────


class _Child(SqlEntityBase):
    firstName: str
    grade: int = 0


class _Pet(SqlEntityBase):
    givenName: str


class _Address(SqlEntityBase):
    city: str
    state: str


class _Profile(SqlEntityBase):
    bio: str
    verified: bool = False


class _Family(RootEntityBase["_Family", str]):
    lastName: str
    children: list[_Child] = []  # noqa: RUF012
    pets: list[_Pet] | None = None
    address: _Address | None = None
    profile: _Profile | None = None
    isRegistered: bool = False
    tags: list[str] = []  # noqa: RUF012


# ── Concrete Repository (bypass __init__ for unit testing) ──────────────────


def _make_repo() -> RepositoryBase:
    """Create a RepositoryBase[_Family, str] without connecting to Cosmos DB."""
    with patch.object(RepositoryBase, "__init__", lambda self, *a, **kw: None):
        repo = _FamilyRepo.__new__(_FamilyRepo)
    return repo


class _FamilyRepo(RepositoryBase[_Family, str]):
    pass  # __init__ is patched out; only _build_sql_query & type helpers needed


# ── _get_field_type_info ────────────────────────────────────────────────────


class TestGetFieldTypeInfo:
    """Verify smart type detection for Pydantic model fields."""

    def test_array_field_detected(self):
        repo = _make_repo()
        info = repo._get_field_type_info("children.firstName")
        assert info["is_array"] is True
        assert info["field_exists"] is True

    def test_optional_array_field_detected(self):
        repo = _make_repo()
        info = repo._get_field_type_info("pets.givenName")
        assert info["is_array"] is True

    def test_single_object_field_detected(self):
        repo = _make_repo()
        info = repo._get_field_type_info("address.city")
        assert info["is_array"] is False
        assert info["field_exists"] is True

    def test_optional_single_field_detected(self):
        repo = _make_repo()
        info = repo._get_field_type_info("profile.bio")
        assert info["is_array"] is False

    def test_nonexistent_field(self):
        repo = _make_repo()
        info = repo._get_field_type_info("nonexistent.field")
        assert info["field_exists"] is False


# ── _is_array_field ─────────────────────────────────────────────────────────


class TestIsArrayField:
    """Verify _is_array_field delegates to _get_field_type_info correctly."""

    def test_array_returns_true(self):
        repo = _make_repo()
        assert repo._is_array_field("children.grade") is True

    def test_single_object_returns_false(self):
        repo = _make_repo()
        assert repo._is_array_field("address.state") is False

    def test_optional_array_returns_true(self):
        repo = _make_repo()
        assert repo._is_array_field("pets.givenName") is True


# ── _build_sql_query: basic equality ───────────────────────────────────────


class TestBuildSqlQueryEquality:
    """Simple equality predicates."""

    def test_single_field(self):
        repo = _make_repo()
        query, params = repo._build_sql_query({"lastName": "Smith"})
        assert "c.lastName = @param0" in query
        assert params["@param0"] == "Smith"

    def test_multiple_fields_and(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"lastName": "Doe", "isRegistered": True}
        )
        assert "c.lastName = @param0" in query
        assert "c.isRegistered = @param1" in query
        assert " AND " in query

    def test_empty_predicate(self):
        repo = _make_repo()
        query, params = repo._build_sql_query({})
        assert "WHERE 1=1" in query
        assert params == {}


# ── _build_sql_query: comparison operators ─────────────────────────────────


class TestBuildSqlQueryComparisons:
    """$eq, $ne, $gt, $gte, $lt, $lte operators."""

    @pytest.mark.parametrize(
        ("op", "sql_op"),
        [
            ("$eq", "="),
            ("$ne", "!="),
            ("$gt", ">"),
            ("$gte", ">="),
            ("$lt", "<"),
            ("$lte", "<="),
        ],
    )
    def test_operator_mapping(self, op, sql_op):
        repo = _make_repo()
        query, params = repo._build_sql_query({"isRegistered": {op: True}})
        assert f"c.isRegistered {sql_op} @param0" in query
        assert params["@param0"] is True

    def test_range_query(self):
        """Two operators on the same field produce two conditions."""
        repo = _make_repo()
        _query, params = repo._build_sql_query({"isRegistered": {"$gte": 1, "$lt": 10}})
        assert "@param0" in params
        assert "@param1" in params


# ── _build_sql_query: $in / $nin ───────────────────────────────────────────


class TestBuildSqlQueryIn:
    """$in and $nin list operators."""

    def test_in_operator(self):
        repo = _make_repo()
        query, params = repo._build_sql_query(
            {"lastName": {"$in": ["Smith", "Doe", "Lee"]}}
        )
        assert "c.lastName IN (" in query
        # param_counter increments before entering $in branch, so params use param1_x
        assert params["@param1_0"] == "Smith"
        assert params["@param1_1"] == "Doe"
        assert params["@param1_2"] == "Lee"

    def test_nin_operator(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query({"lastName": {"$nin": ["X", "Y"]}})
        assert "NOT IN" in query

    def test_in_empty_list_raises(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="at least one value"):
            repo._build_sql_query({"lastName": {"$in": []}})

    def test_in_requires_list(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="list or tuple"):
            repo._build_sql_query({"lastName": {"$in": "not_a_list"}})


# ── _build_sql_query: text operators ───────────────────────────────────────


class TestBuildSqlQueryText:
    """$contains, $startswith, $endswith operators."""

    def test_contains(self):
        repo = _make_repo()
        query, params = repo._build_sql_query({"lastName": {"$contains": "mit"}})
        assert "CONTAINS(c.lastName, @param0)" in query
        assert params["@param0"] == "mit"

    def test_startswith(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query({"lastName": {"$startswith": "Sm"}})
        assert "STARTSWITH(c.lastName, @param0)" in query

    def test_endswith(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query({"lastName": {"$endswith": "th"}})
        assert "ENDSWITH(c.lastName, @param0)" in query


# ── _build_sql_query: $exists ──────────────────────────────────────────────


class TestBuildSqlQueryExists:
    """$exists field existence operator."""

    def test_exists_true(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({"lastName": {"$exists": True}})
        assert "IS_DEFINED(c.lastName)" in query

    def test_exists_false(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({"lastName": {"$exists": False}})
        assert "NOT IS_DEFINED(c.lastName)" in query


# ── _build_sql_query: $and / $or ───────────────────────────────────────────


class TestBuildSqlQueryLogical:
    """$and and $or logical operators."""

    def test_and_operator(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"$and": [{"lastName": "Smith"}, {"isRegistered": True}]}
        )
        assert "$and" not in query  # should be expanded
        assert "AND" in query

    def test_or_operator(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"$or": [{"lastName": "Smith"}, {"lastName": "Doe"}]}
        )
        assert "OR" in query

    def test_and_requires_list(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="list"):
            repo._build_sql_query({"$and": "not_a_list"})

    def test_or_requires_list(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="list"):
            repo._build_sql_query({"$or": "not_a_list"})


# ── _build_sql_query: dot notation (array vs single object) ────────────────


class TestBuildSqlQueryDotNotation:
    """Nested field access via dot notation with smart array detection."""

    def test_array_field_generates_exists_subquery(self):
        """children is List[_Child] → should use EXISTS."""
        repo = _make_repo()
        query, params = repo._build_sql_query({"children.grade": {"$gte": 5}})
        assert "EXISTS(SELECT VALUE p FROM p IN c.children" in query
        assert "p.grade >= @param0" in query
        assert params["@param0"] == 5

    def test_single_object_direct_access(self):
        """address is Optional[_Address] → direct c.address.city access."""
        repo = _make_repo()
        query, _params = repo._build_sql_query({"address.city": "Seattle"})
        assert "c.address.city = @param0" in query
        assert "EXISTS" not in query

    def test_array_simple_equality(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query({"children.firstName": "Alice"})
        assert "EXISTS(SELECT VALUE p FROM p IN c.children" in query
        assert "p.firstName = @param0" in query

    def test_array_contains(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"children.firstName": {"$contains": "Al"}}
        )
        assert "EXISTS(" in query
        assert "CONTAINS(p.firstName, @param0)" in query

    def test_array_startswith(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"children.firstName": {"$startswith": "A"}}
        )
        assert "STARTSWITH(p.firstName, @param0)" in query

    def test_array_endswith(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"children.firstName": {"$endswith": "ce"}}
        )
        assert "ENDSWITH(p.firstName, @param0)" in query

    def test_array_in(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"children.firstName": {"$in": ["Alice", "Bob"]}}
        )
        assert "EXISTS(" in query
        assert "IN (" in query

    def test_array_nin(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query({"children.firstName": {"$nin": ["X"]}})
        assert "EXISTS(" in query
        assert "NOT IN" in query

    def test_array_exists_true(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({"children.grade": {"$exists": True}})
        assert "EXISTS(" in query
        assert "IS_DEFINED(p.grade)" in query

    def test_array_exists_false(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({"children.grade": {"$exists": False}})
        assert "NOT IS_DEFINED(p.grade)" in query

    def test_single_object_contains(self):
        repo = _make_repo()
        query, _params = repo._build_sql_query({"address.city": {"$contains": "Sea"}})
        assert "CONTAINS(c.address.city, @param0)" in query
        assert "EXISTS" not in query

    def test_single_object_exists(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({"address.city": {"$exists": True}})
        assert "IS_DEFINED(c.address.city)" in query
        assert "EXISTS" not in query

    def test_single_object_comparison(self):
        """profile.bio is Optional[_Profile].bio → direct access."""
        repo = _make_repo()
        query, _params = repo._build_sql_query(
            {"profile.bio": {"$startswith": "Software"}}
        )
        assert "STARTSWITH(c.profile.bio, @param0)" in query
        assert "EXISTS" not in query


# ── _build_sql_query: SELECT clause ────────────────────────────────────────


class TestBuildSqlQuerySelectClause:
    """Custom SELECT clauses."""

    def test_default_select_star(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({"lastName": "A"})
        assert query.startswith("SELECT * FROM c")

    def test_custom_select(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query(
            {"lastName": "A"},
            select_clause="SELECT c.id, c._partitionKey",
        )
        assert query.startswith("SELECT c.id, c._partitionKey FROM c")

    def test_count_select(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query(
            {"isRegistered": True},
            select_clause="SELECT VALUE COUNT(1)",
        )
        assert query.startswith("SELECT VALUE COUNT(1) FROM c")


# ── _build_sql_query: ORDER BY ─────────────────────────────────────────────


class TestBuildSqlQuerySorting:
    """ORDER BY clause from sort_fields."""

    def test_single_sort_ascending(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query(
            {},
            sort_fields=[SortField("lastName", SortDirection.ASCENDING)],
        )
        assert "ORDER BY c.lastName ASC" in query

    def test_single_sort_descending(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query(
            {},
            sort_fields=[SortField("lastName", SortDirection.DESCENDING)],
        )
        assert "ORDER BY c.lastName DESC" in query

    def test_multiple_sort_fields(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query(
            {},
            sort_fields=[
                SortField("lastName", SortDirection.ASCENDING),
                SortField("isRegistered", SortDirection.DESCENDING),
            ],
        )
        assert "ORDER BY c.lastName ASC, c.isRegistered DESC" in query

    def test_no_sort_fields(self):
        repo = _make_repo()
        query, _ = repo._build_sql_query({})
        assert "ORDER BY" not in query


class TestBuildSqlQueryFieldSafety:
    """Predicate and sort field names are validated before SQL interpolation."""

    @pytest.mark.parametrize(
        "field_name",
        [
            "lastName; DROP TABLE c",
            "lastName)",
            "lastName[0]",
            "last-name",
            "$where",
        ],
    )
    def test_unsafe_predicate_field_raises(self, field_name):
        repo = _make_repo()
        with pytest.raises(ValueError, match="Invalid SQL field path"):
            repo._build_sql_query({field_name: "Smith"})

    def test_unsafe_nested_logical_field_raises(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="Invalid SQL field path"):
            repo._build_sql_query(
                {"$or": [{"lastName": "Smith"}, {"status; SELECT *": "active"}]}
            )

    def test_operator_keys_are_not_validated_as_field_paths(self):
        repo = _make_repo()
        query, params = repo._build_sql_query({"lastName": {"$gte": "Smith"}})
        assert "c.lastName >= @param0" in query
        assert params["@param0"] == "Smith"

    def test_unsafe_sort_field_raises(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="Invalid SQL field path"):
            repo._build_sql_query(
                {},
                sort_fields=[
                    SortField("lastName DESC; SELECT *", SortDirection.ASCENDING)
                ],
            )


# ── _build_sql_query: unsupported operator ─────────────────────────────────


class TestBuildSqlQueryUnsupported:
    """Unsupported operator raises ValueError."""

    def test_unknown_operator_raises(self):
        repo = _make_repo()
        with pytest.raises(ValueError, match="Unsupported operator"):
            repo._build_sql_query({"lastName": {"$regex": ".*"}})
