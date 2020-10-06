from pathlib import Path
from typing import List, Mapping, Union

from .asyncpg import AsyncPGAdapter
from .exceptions import SQLLoadException
from .queries import Queries
from .query_loader import QueryLoader


async def reload_queries(queries: Queries, path: Path):

    db_url = str(queries.driver_adapter._database_url)

    await queries.disconnect()

    new_queries = from_path(path, url=db_url)

    await new_queries.connect()

    return new_queries


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


def from_path(sql_path: Union[str, Path], *, url: str):
    """Load queries from a `.sql` file, or directory of `.sql` files.

    **Parameters:**

    * **sql_path** - Path to a `.sql` file or directory containing `.sql` files.
    * **driver_adapter** - Either a string to designate one of the aiosql built-in database driver
    adapters. One of "sqlite3", "psycopg2", "aiosqlite", or "asyncpg". If you have defined your own
    adapter class, you may pass its constructor.
    * **record_classes** - *(optional)* **DEPRECATED** Mapping of strings used in "record_class"
    declarations to the python classes which aiosql should use when marshaling SQL results.
    * **loader_cls** - *(optional)* Custom constructor for `QueryLoader` extensions.
    * **queries_cls** - *(optional)* Custom constructor for `Queries` extensions.

    **Returns:** `Queries`

    Usage:

    ```python
    >>> queries = aiosql.from_path("./sql", "pscycopg2")
    >>> queries = aiosql.from_path("./sql", MyDBAdapter)
    ```
    """
    path = Path(sql_path)

    if not path.exists():
        raise SQLLoadException(f"File does not exist: {path}")

    # initiate object
    # always the same object
    pgdriver = AsyncPGAdapter(database_url=url)

    # process queries that object
    query_loader = QueryLoader(pgdriver)  # , record_classes)

    if path.is_file():
        query_data = query_loader.load_query_data_from_file(path)
        return Queries(pgdriver).load_from_list(query_data)
    elif path.is_dir():
        query_data_tree = query_loader.load_query_data_from_dir_path(path)
        return Queries(pgdriver).load_from_tree(query_data_tree)
    else:
        raise SQLLoadException(
            f"The sql_path must be a directory or file, got {sql_path}"
        )
