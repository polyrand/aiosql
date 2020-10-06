from types import MethodType
from typing import Any, Callable, List, Optional, Set, Tuple, cast

from .asyncpg import AsyncPGAdapter
from .types import QueryDataTree, QueryDatum, QueryFn, SQLOperationType


def _params(args, kwargs):
    if len(kwargs) > 0:
        return kwargs
    else:
        return args


def _query_fn(
    fn: Callable[..., Any], name: str, doc: Optional[str], sql: str
) -> QueryFn:
    qfn = cast(QueryFn, fn)
    qfn.__name__ = name
    qfn.__doc__ = doc
    qfn.sql = sql
    return qfn


def _make_fn(query_datum: QueryDatum) -> QueryFn:
    query_name, doc_comments, operation_type, sql = query_datum
    if operation_type == SQLOperationType.INSERT_RETURNING:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.insert_returning(
                query_name, sql, _params(args, kwargs)
            )

    elif operation_type == SQLOperationType.INSERT_UPDATE_DELETE:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.insert_update_delete(
                query_name, sql, _params(args, kwargs)
            )

    elif operation_type == SQLOperationType.INSERT_UPDATE_DELETE_MANY:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.insert_update_delete_many(
                query_name, sql, *_params(args, kwargs)
            )

    elif operation_type == SQLOperationType.SCRIPT:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.execute_script(sql)

    elif operation_type == SQLOperationType.SELECT:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.select(
                query_name, sql, _params(args, kwargs)
            )

    elif operation_type == SQLOperationType.SELECT_ONE:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.select_one(
                query_name, sql, _params(args, kwargs)
            )

    elif operation_type == SQLOperationType.SELECT_VALUE:

        async def fn(self: Queries, *args, **kwargs):
            return await self.driver_adapter.select_value(
                query_name, sql, _params(args, kwargs)
            )

    else:
        raise ValueError(f"Unknown operation_type: {operation_type}")

    return _query_fn(fn, query_name, doc_comments, sql)


def _make_ctx_mgr(fn: QueryFn) -> QueryFn:
    def ctx_mgr(self, *args, **kwargs):
        return self.driver_adapter.select_cursor(
            fn.__name__, fn.sql, _params(args, kwargs)
        )

    return _query_fn(ctx_mgr, f"{fn.__name__}_cursor", fn.__doc__, fn.sql)


def _create_methods(query_datum: QueryDatum) -> List[Tuple[str, QueryFn]]:
    fn = _make_fn(query_datum)

    ctx_mgr = _make_ctx_mgr(fn)

    if query_datum.operation_type == SQLOperationType.SELECT:
        return [(fn.__name__, fn), (ctx_mgr.__name__, ctx_mgr)]
    else:
        return [(fn.__name__, fn)]


class Queries:
    """Container object with dynamic methods built from SQL queries.

    The `-- name` definition comments in the content of the SQL determine what the dynamic
    methods of this class will be named.

    **Parameters:**

    * **driver_adapter** - Either a string to designate one of the aiosql built-in database driver
    adapters. One of "sqlite3", "psycopg2", "aiosqlite", or "asyncpg". If you have defined your
    own adapter class, you can pass it's constructor.
    """

    def __init__(self, driver_adapter: AsyncPGAdapter):
        self.driver_adapter = driver_adapter
        # self._url = url
        self._available_queries: Set[str] = set()

    async def connect(self):
        await self.driver_adapter.connect()

    async def disconnect(self):
        await self.driver_adapter.disconnect()

    @property
    def available_queries(self) -> List[str]:
        """Returns listing of all the available query methods loaded in this class.

        **Returns:** `List[str]` List of dot-separated method accessor names.
        """
        return sorted(self._available_queries)

    def __repr__(self):
        return "Queries(" + self.available_queries.__repr__() + ")"

    def add_query(self, query_name: str, fn: Callable):
        """Adds a new dynamic method to this class.

        **Parameters:**

        * **query_name** - The method name as found in the SQL content.
        * **fn** - The loaded query function.
        """
        setattr(self, query_name, fn)
        self._available_queries.add(query_name)

    def add_queries(self, queries: List[Tuple[str, QueryFn]]):
        """Add query methods to `Queries` instance."""
        for query_name, fn in queries:
            self.add_query(query_name, MethodType(fn, self))

    def add_child_queries(self, child_name: str, child_queries: "Queries"):
        """Adds a Queries object as a property.

        **Parameters:**

        * **child_name** - The property name to group the child queries under.
        * **child_queries** - Queries instance to add as sub-queries.
        """
        setattr(self, child_name, child_queries)
        for child_query_name in child_queries.available_queries:
            self._available_queries.add(f"{child_name}.{child_query_name}")

    def load_from_list(self, query_data: List[QueryDatum]):
        """Load Queries from a list of `QuaryDatum`"""
        for query_datum in query_data:
            self.add_queries(_create_methods(query_datum))
        return self

    def load_from_tree(self, query_data_tree: QueryDataTree):
        """Load Queries from a `QuaryDataTree`"""
        for key, value in query_data_tree.items():
            if isinstance(value, dict):
                self.add_child_queries(
                    key, Queries(self.driver_adapter).load_from_tree(value)
                )
            else:
                self.add_queries(_create_methods(value))
        return self
