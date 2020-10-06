from collections import defaultdict
import asyncio
import contextlib
import functools
import logging
import sys
import typing
from types import TracebackType
from urllib.parse import SplitResult, parse_qsl, urlsplit


from contextvars import ContextVar

LOG_EXTRA = {}
CONNECT_EXTRA = {}
DISCONNECT_EXTRA = {}


logger = logging.getLogger("databases")

import logging
import typing
from collections.abc import Mapping

import asyncpg


from contextlib import asynccontextmanager
from .patterns import var_pattern


logger = logging.getLogger("databases")


# _result_processors = {}  # type: dict


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


class DoAcquire:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        self._managed_conn = await self.client.acquire()
        return self._managed_conn

    async def __aexit__(self, exc_type, exc, tb):
        assert self._managed_conn is not None
        await self.client.release(self._managed_conn)


class AsyncPGAdapter:
    is_aio_driver = True

    def __init__(self, database_url: str, **options: typing.Any):
        self.var_replacements = defaultdict(dict)
        self._database_url = DatabaseURL(database_url)
        self._options = options
        self._connection = None  # type: typing.Optional[asyncpg.connection.Connection]
        self._pool = None

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

    async def select(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with DoAcquire(self._pool) as connection:
            results = await connection.fetch(sql, *parameters)
            # if record_class is not None:
            #     results = [record_class(**dict(rec)) for rec in results]
        return results

    async def select_one(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with DoAcquire(self._pool) as connection:
            result = await connection.fetchrow(sql, *parameters)
        return result

    async def select_value(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with DoAcquire(self._pool) as connection:
            return await connection.fetchval(sql, *parameters)

    @asynccontextmanager
    async def select_cursor(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with DoAcquire(self._pool) as connection:
            stmt = await connection.prepare(sql)
            async with connection.transaction():
                yield stmt.cursor(*parameters)

    async def insert_returning(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with DoAcquire(self._pool) as connection:
            res = await connection.fetchrow(sql, *parameters)
            if res:
                return res[0] if len(res) == 1 else res
            else:
                return None

    async def insert_update_delete(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = self.maybe_order_params(query_name, parameters)
        async with DoAcquire(self._pool) as connection:
            await connection.execute(sql, *parameters)

    async def insert_update_delete_many(self, query_name, sql, parameters):
        assert self._pool is not None, "Connection is not acquired"
        parameters = [
            self.maybe_order_params(query_name, params) for params in parameters
        ]
        async with DoAcquire(self._pool) as connection:
            await connection.executemany(sql, parameters)

    async def execute_script(self, sql):
        assert self._pool is not None, "Connection is not acquired"
        async with DoAcquire(self._pool) as connection:
            return await connection.execute(sql)

    # @property
    # def raw_connection(self) -> asyncpg.connection.Connection:
    #     assert self._connection is not None, "Connection is not acquired"
    #     return self._connection
