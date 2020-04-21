from collections import OrderedDict as od
import sqlparse
from schemainspect.pg.obj import InspectedRole


def extract_roles(raw):
    statements = sqlparse.split(raw)
    roleStatements = []
    other = ""

    for statement in statements:
        if not statement:
            continue

        if statement.startswith("CREATE ROLE"):
            parsed = sqlparse.parse(statement)
            roleStatements.append(parsed)
        else:
            other += statement + "\n"

    return roles(roleStatements), other


def roles(statements):
    roles = []

    for statement in statements:
        # [create][ ][role][ ][<name>] <rest>
        _, _, _, _, maybe_name, *rest = statement[0].tokens
        name, *additional = str(maybe_name).split(" ")

        role = InspectedRole(
            name,
            "NOSUPERUSER",
            "NOCREATEDB",
            "NOCREATEROLE",
            "INHERIT",
            "NOLOGIN",
            "NOREPLICATION",
            "NOBYPASSRLS",
            -1,
            None,
            None,
        )

        options = additional + rest
        i = 0

        while i < len(options):
            option = str(options[i])
            i += 1

            if option.startswith("LOGIN "):
                new_options = option.split(" ")
                options[i:i] = new_options
                continue

            if option == "PASSWORD":
                role.password = str(options[i + 1]).strip("'")

                if role.password == "NULL" or role.password == "":
                    role.password = None

                i += 1
                continue

            if option == "VALID":
                role.valid_until = str(options[i + 3]).strip("'")
                i += 3
                continue

            if option == "CONNECTION":
                role.connection_limit = int(str(options[i + 3]))
                i += 3

            if option == "SUPERUSER" or option == "NOSUPERUSER":
                role.superuser = option

            if option == "CREATEDB" or option == "NOCREATEDB":
                role.createdb = option

            if option == "INHERIT" or option == "NOINHERIT":
                role.inherit = option

            if option == "LOGIN" or option == "NOLOGIN":
                role.login = option

            if option == "REPLICATION" or option == "NOREPLICATION":
                role.replication = option

            if option == "BYPASSRLS" or option == "NOBYPASSRLS":
                role.bypassrls = option

            if option == "LOGIN" or option == "NOLOGIN":
                role.login = option

        roles.append(role)

    return od((r.name, r) for r in roles)
