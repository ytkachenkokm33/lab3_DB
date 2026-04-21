from __future__ import annotations

import argparse
import getpass

import psycopg
from psycopg import sql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create PostgreSQL role and database for the weather lab."
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5432, type=int)
    parser.add_argument("--admin-user", default="postgres")
    parser.add_argument("--database", default="weather_lab")
    parser.add_argument("--app-user", default="weather")
    parser.add_argument("--app-password", default="weather")
    return parser.parse_args()


def role_exists(connection: psycopg.Connection, role_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("select 1 from pg_roles where rolname = %s", (role_name,))
        return cursor.fetchone() is not None


def database_exists(connection: psycopg.Connection, database_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("select 1 from pg_database where datname = %s", (database_name,))
        return cursor.fetchone() is not None


def main() -> int:
    args = parse_args()
    admin_password = getpass.getpass(
        f"Password for PostgreSQL admin user '{args.admin_user}': "
    )

    connection_info = {
        "host": args.host,
        "port": args.port,
        "dbname": "postgres",
        "user": args.admin_user,
        "password": admin_password,
    }

    with psycopg.connect(**connection_info) as connection:
        connection.autocommit = True

        if role_exists(connection, args.app_user):
            print(f"Role '{args.app_user}' already exists.")
        else:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("create role {} with login password {}").format(
                        sql.Identifier(args.app_user),
                        sql.Literal(args.app_password),
                    )
                )
            print(f"Created role '{args.app_user}'.")

        if database_exists(connection, args.database):
            print(f"Database '{args.database}' already exists.")
        else:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("create database {} owner {}").format(
                        sql.Identifier(args.database),
                        sql.Identifier(args.app_user),
                    )
                )
            print(f"Created database '{args.database}'.")

    print(
        "DATABASE_URL="
        f"postgresql+psycopg://{args.app_user}:{args.app_password}"
        f"@{args.host}:{args.port}/{args.database}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
