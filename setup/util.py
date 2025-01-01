import json
import os
from pathlib import Path

import pglast
from sqlalchemy import Connection, Engine, Inspector, inspect, text


def connstr() -> str:
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")
    pg_db = os.getenv("PG_DB")
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT")
    return f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"


def dump_pg_stats(connection: Connection, filepath: Path):
    result = conn_execute(connection, "SELECT row_to_json(s) from pg_stats s", verbose=False)
    with open(filepath, "w") as f:
        for row in result.fetchall():
            stats = row[0]
            if stats["schemaname"].startswith("pg_") or stats["schemaname"] == "information_schema":
                continue
            json_str = json.dumps(row[0])
            print(json_str, file=f)


def sql_file_queries(filepath: Path) -> [str]:
    with open(filepath) as f:
        lines = []
        for line in f:
            if line.startswith("--"):
                continue
            if len(line.strip()) == 0:
                continue
            lines.append(line)
        queries = "".join(lines)
        return pglast.split(queries)


def sql_file_execute(connection: Connection, filepath: Path, verbose=True) -> None:
    for sql in sql_file_queries(filepath):
        conn_execute(connection, sql, verbose=verbose)


def conn_execute(connection: Connection, sql: str, verbose=True):
    if verbose:
        print(sql)
    return connection.execute(text(sql))


def prewarm_all(engine: Engine, connection: Connection, verbose=True):
    inspector: Inspector = inspect(engine)
    conn_execute(
        connection, "CREATE EXTENSION IF NOT EXISTS pg_prewarm", verbose=verbose
    )
    for table in inspector.get_table_names():
        conn_execute(connection, f"SELECT pg_prewarm('{table}')", verbose=verbose)
        for index in inspector.get_indexes(table):
            index_name = index["name"]
            conn_execute(
                connection, f"SELECT pg_prewarm('{index_name}')", verbose=verbose
            )


def vacuum_analyze_all(engine: Engine, connection: Connection, verbose=True):
    inspector: Inspector = inspect(engine)
    for table in inspector.get_table_names():
        conn_execute(connection, f"VACUUM ANALYZE {table}", verbose=verbose)


def vacuum_full_analyze_all(engine: Engine, connection: Connection, verbose=True):
    inspector: Inspector = inspect(engine)
    for table in inspector.get_table_names():
        conn_execute(connection, f"VACUUM FULL ANALYZE {table}", verbose=verbose)


if __name__ == "__main__":
    from sqlalchemy import create_engine, NullPool
    engine: Engine = create_engine(
        "postgresql+psycopg://peft_user:peft_pass@localhost:15721/peft_db_tpch_sf_10",
        poolclass=NullPool,
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )
    with engine.connect() as connection:
        dump_pg_stats(connection, "./dumptest.json")
