import re
import logging
import typing
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path
from types import MethodType
from typing import Any, Callable, List, Optional, Set, Tuple, cast, Dict, Union, Mapping
from urllib.parse import SplitResult, parse_qsl, urlsplit
from dataclasses import asdict, is_dataclass

import asyncpg

from enum import Enum
from typing import Any, NamedTuple

from typing_extensions import Protocol

logger = logging.getLogger("databases")

__all__ = [
    "AsyncPGAdapter",
    "toclass",
    "SQLParseException",
    "SQLLoadException",
]


## ¡¡types


class SQLOperationType(Enum):
    """Enumeration of aiosql operation types."""

    INSERT_RETURNING = 0
    INSERT_UPDATE_DELETE = 1
    INSERT_UPDATE_DELETE_MANY = 2
    SCRIPT = 3
    SELECT = 4
    SELECT_ONE = 5
    SELECT_VALUE = 6


class QueryDatum(NamedTuple):
    query_name: str
    doc_comments: str
    operation_type: SQLOperationType
    sql: str
    # record_class: Any = None


class QueryFn(Protocol):
    __name__: str
    sql: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        ...


# Can't make this a recursive type in terms of itself
# QueryDataTree = Dict[str, Union[QueryDatum, 'QueryDataTree']]
QueryDataTree = Dict[str, Union[QueryDatum, Dict]]

## ¡¡patterns

query_name_definition_pattern = re.compile(r"--\s*name\s*:\s*")
"""
Pattern: Identifies name definition comments.
"""

query_record_class_definition_pattern = re.compile(r"--\s*record_class\s*:\s*(\w+)\s*")
"""
Pattern: Identifies record_class definition comments.
"""

empty_pattern = re.compile(r"^\s*$")
"""
Pattern: Identifies empty lines.
"""

valid_query_name_pattern = re.compile(r"^\w+$")
"""
Pattern: Enforces names are valid python variable names.
"""

doc_comment_pattern = re.compile(r"\s*--\s*(.*)$")
"""
Pattern: Identifies SQL comments.
"""

var_pattern = re.compile(
    r'(?P<dblquote>"[^"]+")|'
    r"(?P<quote>\'[^\']+\')|"
    r"(?P<lead>[^:]):(?P<var_name>[\w-]+)(?P<trail>[^:]?)"
)
"""
Pattern: Identifies variable definitions in SQL code.
"""

## ¡¡exceptions


class SQLLoadException(Exception):
    """Raised when there is a problem loading SQL content from a file or directory."""


class SQLParseException(Exception):
    """Raised when there was a problem parsing the aiosql comment annotations in SQL"""


LOG_EXTRA = {}
CONNECT_EXTRA = {}
DISCONNECT_EXTRA = {}

## ¡¡functions for query load


def _params(args, kwargs):
    if len(kwargs) > 0:
        return kwargs
    else:
        if len(args) == 1:
            arg = args[0]
            # pydantic model or dataclass
            if str(
                type(type(arg))
            ) == "<class 'pydantic.main.ModelMetaclass'>" or is_dataclass(arg):
                # return the class, it will be processed later
                # in the maybe_order_params function
                return arg

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

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.insert_returning(query_name, sql, _params(args, kwargs))

    elif operation_type == SQLOperationType.INSERT_UPDATE_DELETE:

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.insert_update_delete(
                query_name, sql, _params(args, kwargs)
            )

    elif operation_type == SQLOperationType.INSERT_UPDATE_DELETE_MANY:

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.insert_update_delete_many(
                query_name, sql, *_params(args, kwargs)
            )

    elif operation_type == SQLOperationType.SCRIPT:

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.execute_script(sql)

    elif operation_type == SQLOperationType.SELECT:

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.select(query_name, sql, _params(args, kwargs))

    elif operation_type == SQLOperationType.SELECT_ONE:

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.select_one(query_name, sql, _params(args, kwargs))

    elif operation_type == SQLOperationType.SELECT_VALUE:

        async def fn(self: AsyncPGAdapter, *args, **kwargs):
            return await self.select_value(query_name, sql, _params(args, kwargs))

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


## ¡¡utilities


def toclass(results: Union[List[Mapping], Mapping], obj):

    # obj is dataclasses
    if isinstance(obj, type):
        if isinstance(results, list):
            return [obj(**dict(r)) for r in results]
        else:
            return obj(**dict(results))

    # obj is pydantic model
    # I'm using this hacky str(type()) to avoid having pydantic
    # as a dependency
    elif type(obj) == "<class 'pydantic.main.ModelMetaclass'>":
        if isinstance(results, list):
            return [obj.parse_obj(r) for r in results]
        else:
            return obj.parse_obj(results)


# copied from: https://github.com/encode/databases
class _EmptyNetloc(str):
    def __bool__(self) -> bool:
        return True


class DatabaseURL:
    def __init__(self, url: typing.Union[str, "DatabaseURL"]):
        self._url = str(url)

    @property
    def components(self) -> SplitResult:
        if not hasattr(self, "_components"):
            self._components = urlsplit(self._url)
        return self._components

    @property
    def scheme(self) -> str:
        return self.components.scheme

    @property
    def dialect(self) -> str:
        return self.components.scheme.split("+")[0]

    @property
    def driver(self) -> str:
        if "+" not in self.components.scheme:
            return ""
        return self.components.scheme.split("+", 1)[1]

    @property
    def username(self) -> typing.Optional[str]:
        return self.components.username

    @property
    def password(self) -> typing.Optional[str]:
        return self.components.password

    @property
    def hostname(self) -> typing.Optional[str]:
        return self.components.hostname

    @property
    def port(self) -> typing.Optional[int]:
        return self.components.port

    @property
    def netloc(self) -> typing.Optional[str]:
        return self.components.netloc

    @property
    def database(self) -> str:
        path = self.components.path
        if path.startswith("/"):
            path = path[1:]
        return path

    @property
    def options(self) -> dict:
        if not hasattr(self, "_options"):
            self._options = dict(parse_qsl(self.components.query))
        return self._options

    def replace(self, **kwargs: typing.Any) -> "DatabaseURL":
        if (
            "username" in kwargs
            or "password" in kwargs
            or "hostname" in kwargs
            or "port" in kwargs
        ):
            hostname = kwargs.pop("hostname", self.hostname)
            port = kwargs.pop("port", self.port)
            username = kwargs.pop("username", self.username)
            password = kwargs.pop("password", self.password)

            netloc = hostname
            if port is not None:
                netloc += f":{port}"
            if username is not None:
                userpass = username
                if password is not None:
                    userpass += f":{password}"
                netloc = f"{userpass}@{netloc}"

            kwargs["netloc"] = netloc

        if "database" in kwargs:
            kwargs["path"] = "/" + kwargs.pop("database")

        if "dialect" in kwargs or "driver" in kwargs:
            dialect = kwargs.pop("dialect", self.dialect)
            driver = kwargs.pop("driver", self.driver)
            kwargs["scheme"] = f"{dialect}+{driver}" if driver else dialect

        if not kwargs.get("netloc", self.netloc):
            # Using an empty string that evaluates as True means we end up
            # with URLs like `sqlite:///database` instead of `sqlite:/database`
            kwargs["netloc"] = _EmptyNetloc()

        components = self.components._replace(**kwargs)
        return self.__class__(components.geturl())

    @property
    def obscure_password(self) -> str:
        if self.password:
            return self.replace(password="********")._url
        return self._url

    def __str__(self) -> str:
        return self._url

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.obscure_password)})"

    def __eq__(self, other: typing.Any) -> bool:
        return str(self) == str(other)


class MaybeAcquire:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        try:
            self._managed_conn = await self.client.acquire()
            return self._managed_conn
        except AttributeError:
            self._managed_conn = None
            return self.client

    async def __aexit__(self, exc_type, exc, tb):
        # assert self._managed_conn is not None
        if self._managed_conn is not None:
            await self.client.release(self._managed_conn)


class AsyncPGAdapter:
    def __init__(self, database_url: str, **options: typing.Any):
        self.var_replacements = defaultdict(dict)
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._connection = None  # type: typing.Optional[asyncpg.connection.Connection]
        self._pool = None
        self._available_queries: Set[str] = set()

    @property
    def available_queries(self) -> List[str]:
        """Returns listing of all the available query methods loaded in this class.

        **Returns:** `List[str]` List of dot-separated method accessor names.
        """
        return sorted(self._available_queries)

    def __repr__(self):
        return "Queries(" + self.available_queries.__repr__() + ")"

    def load_all(self, sql_path: Union[str, Path]):
        path = Path(sql_path)

        if not path.exists():
            raise SQLLoadException(f"File does not exist: {path}")

        if path.is_file():
            query_data = self.load_query_data_from_file(path)
            self.load_from_list(query_data)
        elif path.is_dir():
            query_data_tree = self.load_query_data_from_dir_path(path)
            self.load_from_tree(query_data_tree)
        else:
            raise SQLLoadException(
                f"The sql_path must be a directory or file, got {sql_path}"
            )

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

    def add_child_queries(self, child_name: str, child_queries):
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
                self.add_child_queries(key, self.load_from_tree(value))
            else:
                self.add_queries(_create_methods(value))
        return self

    def _make_query_datum(self, query_str: str):
        lines = [line.strip() for line in query_str.strip().splitlines()]
        query_name = lines[0].replace("-", "_")

        if query_name.endswith("<!"):
            operation_type = SQLOperationType.INSERT_RETURNING
            query_name = query_name[:-2]
        elif query_name.endswith("*!"):
            operation_type = SQLOperationType.INSERT_UPDATE_DELETE_MANY
            query_name = query_name[:-2]
        elif query_name.endswith("!"):
            operation_type = SQLOperationType.INSERT_UPDATE_DELETE
            query_name = query_name[:-1]
        elif query_name.endswith("#"):
            operation_type = SQLOperationType.SCRIPT
            query_name = query_name[:-1]
        elif query_name.endswith("^"):
            operation_type = SQLOperationType.SELECT_ONE
            query_name = query_name[:-1]
        elif query_name.endswith("$"):
            operation_type = SQLOperationType.SELECT_VALUE
            query_name = query_name[:-1]
        else:
            operation_type = SQLOperationType.SELECT

        if not valid_query_name_pattern.match(query_name):
            raise SQLParseException(
                f'name must convert to valid python variable, got "{query_name}".'
            )

        line_offset = 1

        doc_comments = ""
        sql = ""
        for line in lines[line_offset:]:
            doc_match = doc_comment_pattern.match(line)
            if doc_match:
                doc_comments += doc_match.group(1) + "\n"
            else:
                sql += line + "\n"

        doc_comments = doc_comments.strip()
        sql = self.process_sql(query_name, operation_type, sql.strip())

        return QueryDatum(query_name, doc_comments, operation_type, sql)

    def load_query_data_from_sql(self, sql: str) -> List[QueryDatum]:
        query_data = []
        query_sql_strs = query_name_definition_pattern.split(sql)

        # Drop the first item in the split. It is anything above the first query definition.
        # This may be SQL comments or empty lines.
        # See: https://github.com/nackjicholson/aiosql/issues/35
        for query_sql_str in query_sql_strs[1:]:
            query_data.append(self._make_query_datum(query_sql_str))
        return query_data

    def load_query_data_from_file(self, file_path: Path) -> List[QueryDatum]:
        with file_path.open() as fp:
            return self.load_query_data_from_sql(fp.read())

    def load_query_data_from_dir_path(self, dir_path) -> QueryDataTree:
        if not dir_path.is_dir():
            raise ValueError(f"The path {dir_path} must be a directory")

        def _recurse_load_query_data_tree(path):
            # queries = Queries()
            query_data_tree = {}
            for p in path.iterdir():
                if p.is_file() and p.suffix != ".sql":
                    continue
                elif p.is_file() and p.suffix == ".sql":
                    for query_datum in self.load_query_data_from_file(p):
                        query_data_tree[query_datum.query_name] = query_datum
                elif p.is_dir():
                    child_name = p.relative_to(dir_path).name
                    child_query_data_tree = _recurse_load_query_data_tree(p)
                    query_data_tree[child_name] = child_query_data_tree
                else:
                    # This should be practically unreachable.
                    raise SQLLoadException(
                        f"The path must be a directory or file, got {p}"
                    )
            return query_data_tree

        return _recurse_load_query_data_tree(dir_path)

    def _get_connection_kwargs(self) -> dict:
        url_options = self._database_url.options

        kwargs = {}
        min_size = url_options.get("min_size")
        max_size = url_options.get("max_size")
        ssl = url_options.get("ssl")

        if min_size is not None:
            kwargs["min_size"] = int(min_size)
        if max_size is not None:
            kwargs["max_size"] = int(max_size)
        if ssl is not None:
            kwargs["ssl"] = {"true": True, "false": False}[ssl.lower()]

        kwargs.update(self._options)

        return kwargs

    async def connect(self) -> None:
        assert self._pool is None, "DatabaseBackend is already running"
        kwargs = self._get_connection_kwargs()
        logger.debug("Creating connection pool")
        self._pool = await asyncpg.create_pool(
            host=self._database_url.hostname,
            port=self._database_url.port,
            user=self._database_url.username,
            password=self._database_url.password,
            database=self._database_url.database,
            **kwargs,
        )

        logger.info("Connected to database %s", self._database_url.obscure_password)

    async def disconnect(self) -> None:
        assert self._pool is not None, "DatabaseBackend is not running"
        await self._pool.close()
        self._pool = None

        logger.info(
            "Disconnected from database %s", self._database_url.obscure_password,
        )

    def process_sql(self, query_name, _op_type, sql):
        count = 0
        adj = 0

        for match in var_pattern.finditer(sql):
            gd = match.groupdict()
            # Do nothing if the match is found within quotes.
            if gd["dblquote"] is not None or gd["quote"] is not None:
                continue

            var_name = gd["var_name"]
            if var_name in self.var_replacements[query_name]:
                replacement = f"${self.var_replacements[query_name][var_name]}"
            else:
                count += 1
                replacement = f"${count}"
                self.var_replacements[query_name][var_name] = count

            start = match.start() + len(gd["lead"]) + adj
            end = match.end() - len(gd["trail"]) + adj

            sql = sql[:start] + replacement + sql[end:]

            replacement_len = len(replacement)
            # the lead ":" char is the reason for the +1
            var_len = len(var_name) + 1
            if replacement_len < var_len:
                adj = adj + replacement_len - var_len
            else:
                adj = adj + var_len - replacement_len

        logger.debug("Query: %s Args: %s", sql, extra=LOG_EXTRA)

        return sql

    def maybe_order_params(self, query_name, parameters):

        # hacky way to check if its a pydantic model without importing it
        if str(type(type(parameters))) == "<class 'pydantic.main.ModelMetaclass'>":
            parameters = parameters.dict()

        # or it's a dataclass
        elif is_dataclass(parameters):
            parameters = asdict(parameters)

        if isinstance(parameters, dict):
            xs = [
                (self.var_replacements[query_name][k], v) for k, v in parameters.items()
            ]
            xs = sorted(xs, key=lambda x: x[0])
            return [x[1] for x in xs]
        elif isinstance(parameters, tuple):
            return parameters
        else:
            raise ValueError(
                f"Parameters expected to be dict or tuple, received {parameters}"
            )

    # SQL operations start here
    async def select(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(self._pool) as connection:
            results = await connection.fetch(sql, *parameters)
        return results

    async def select_one(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(self._pool) as connection:
            result = await connection.fetchrow(sql, *parameters)
        return result

    async def select_value(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(self._pool) as connection:
            return await connection.fetchval(sql, *parameters)

    @asynccontextmanager
    async def select_cursor(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(self._pool) as connection:
            stmt = await connection.prepare(sql)
            async with connection.transaction():
                yield stmt.cursor(*parameters)

    async def insert_returning(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(self._pool) as connection:
            res = await connection.fetchrow(sql, *parameters)
            if res:
                return res[0] if len(res) == 1 else res
            else:
                return None

    async def insert_update_delete(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with MaybeAcquire(self._pool) as connection:
            await connection.execute(sql, *parameters)

    async def insert_update_delete_many(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = [
            self.maybe_order_params(query_name, params) for params in parameters
        ]
        async with MaybeAcquire(self._pool) as connection:
            await connection.executemany(sql, parameters)

    async def execute_script(self, sql):
        assert self._pool is not None, "Connection is not acquired"
        async with MaybeAcquire(self._pool) as connection:
            return await connection.execute(sql)

    def reload(self, sql_path: Union[str, Path]):
        self.load_all(sql_path=sql_path)
