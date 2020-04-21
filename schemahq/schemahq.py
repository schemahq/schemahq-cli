import os
import sys
import random
import string
from contextlib import contextmanager
from unittest.mock import patch

from pgformatter import pg_format
import colorful as cf
from sqlalchemy.pool import NullPool
from sqlbag import (
    C,
    S,
    session,
    database_exists,
    create_database,
    drop_database,
    load_sql_from_file,
    sql_from_file,
    raw_execute,
    copy_url,
)
from migra import Migration, UnsafeMigrationException
from migra.changes import statements_for_changes

from .roles import extract_roles

cf.use_style("solarized")


def temporary_name(prefix="migra_"):
    random_letters = [random.choice(string.ascii_lowercase) for _ in range(10)]
    rnd = "".join(random_letters)
    tempname = prefix + rnd
    return tempname


@contextmanager
def temporary_database(base_uri):
    # Create temporary database
    tempname = temporary_name()
    temp_uri = copy_url(base_uri)
    temp_uri.database = tempname
    create_database(temp_uri)
    s = session(temp_uri)

    try:
        yield s
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()
        drop_database(temp_uri)


def create_admin_patch(db_uri):
    @contextmanager
    def admin_connection(_):
        with C(db_uri, poolclass=NullPool, isolation_level="AUTOCOMMIT") as c:
            yield c

    return patch("sqlbag.createdrop.admin_db_connection", new=admin_connection)


def diff(
    schema: str, db: str, unsafe: bool = False, apply: bool = False,
):
    """
    Diff a file of SQL statements against a database.
    :param schema: The SQL schema to match against.
    :param db: The database to target.
    :param unsafe: Generate unsafe statements.
    :param apply: Apply the statements to bring the target database up to date.
    """
    if not os.path.exists(schema):
        print(cf.bold_red("Error:"), f'Could not find file "{schema}"', file=sys.stderr)
        sys.exit(os.EX_OSFILE)

    base_uri = copy_url(db)
    target_exists = database_exists(base_uri, test_can_select=True)

    if not target_exists:
        print(
            cf.bold_red("Error:"), f'Database "{base_uri.database}" does not exist.',
        )
        sys.exit(os.EX_NOHOST)

    patch = create_admin_patch(base_uri)
    patch.start()

    with temporary_database(base_uri) as sTemp, S(db) as sFrom:
        # Run schema in temporary database
        roles = []

        try:
            roles, statements = extract_roles(sql_from_file(schema))
            raw_execute(sTemp, statements)
        except Exception as e:
            print(cf.bold_red("Error:"), e, file=sys.stderr)
            sys.exit(os.EX_DATAERR)

        # Compare roles
        # get_inspector()

        # Compare
        m = Migration(sFrom, sTemp)
        m.add_all_changes(privileges=True)
        m.add(statements_for_changes(m.changes.i_from.roles, roles))

        if not m.statements:
            print(cf.bold("All done! ✨"))
            print(f'Database "{base_uri.database}" is up to date.')
            sys.exit()

        sql = ""

        if unsafe:
            m.set_safety(False)

        # Get SQL
        try:
            sql = m.sql
        except UnsafeMigrationException:
            print(
                cf.bold_yellow("Careful:"),
                "Unsafe statements generated.",
                file=sys.stderr,
            )
            print("Run again with", cf.bold("--unsafe"))
            sys.exit(os.EX_USAGE)

        print(pg_format(sql.encode(), unquote=True).decode())

        if apply:
            print(cf.bold("Applying..."))
            m.apply()
            print(cf.bold("All done! ✨"))
            print(f'Database "{base_uri.database}" has been updated.')


def apply_statements(statements: str, db: str):
    """
    Apply a file of SQL statements to a database.
    :param statements: An SQL file of statements to apply to the database.
    :param db: A database to target.
    """
    if not os.path.exists(statements):
        print(
            cf.bold_red("Error:"),
            f'Could not find file "{statements}"',
            file=sys.stderr,
        )
        sys.exit(os.EX_OSFILE)

    base_uri = copy_url(db)
    target_exists = database_exists(base_uri, test_can_select=True)

    if not target_exists:
        print(
            cf.bold_red("Error:"), f'Database "{base_uri.database}" does not exist.',
        )
        sys.exit(os.EX_NOHOST)

    with S(db) as s:
        try:
            load_sql_from_file(s, statements)
        except Exception as e:
            print(cf.bold_red("Error:"), e, file=sys.stderr)
            sys.exit(os.EX_DATAERR)

    print(cf.bold("All done! ✨"))


def init(db: str = None, schema: str = "schema.sql", overwrite: bool = False):
    """
    Create an initial schema SQL file, optionally from an existing database.
    :param db: An optional database to create the schema from.
    :param schema: An optional file to write schema to. Default: schema.sql
    :param overwrite: Overwrite existing file.
    """
    if os.path.exists(schema) and not overwrite:
        print(
            cf.bold_red("Error:"), f'File "{schema}" already exists.', file=sys.stderr
        )
        print("Run again with", cf.bold("--overwrite"), "to replace.")
        sys.exit(os.EX_OSFILE)

    if not db:
        with open(schema, "w") as f:
            f.write("")

        print(cf.bold("All done! ✨"))
        print(f'Created blank file "{schema}"')
        sys.exit()

    base_uri = copy_url(db)
    target_exists = database_exists(base_uri, test_can_select=True)

    if not target_exists:
        print(
            cf.bold_red("Error:"), f'Database "{base_uri.database}" does not exist.',
        )
        sys.exit(os.EX_NOHOST)

    sql = ""

    patch = create_admin_patch(base_uri)
    patch.start()

    with temporary_database(base_uri) as sTemp, S(db) as sFrom:
        # Compare
        m = Migration(sTemp, sFrom)
        m.add_all_changes()
        m.set_safety(False)

        # Get SQL
        sql = m.sql

    with open(schema, "wb") as f:
        f.write(pg_format(sql.encode(), unquote=True))

    print(cf.bold("All done! ✨"))
    print(f'Created file "{schema}" with schema from "{base_uri.database}"')
    sys.exit()


schemahq = {
    "apply": apply_statements,
    "diff": diff,
    "init": init,
}
