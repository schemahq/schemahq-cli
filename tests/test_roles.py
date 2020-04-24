from schemahq.roles import extract_roles


def test_extract_roles():
    roles, other = extract_roles(
        """
CREATE ROLE admin;
CREATE ROLE jonathan LOGIN;
CREATE ROLE davide WITH PASSWORD 'jw8s0F4';
CREATE ROLE miriam WITH LOGIN PASSWORD 'jw8s0F4' VALID UNTIL '2005-01-01';
CREATE ROLE admin2 WITH CREATEDB CREATEROLE;
CREATE TABLE films (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(40) NOT NULL,
    did         integer NOT NULL,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute
);
SELECT kind, sum(len) AS total FROM films GROUP BY kind;
"""
    )

    assert len(roles) == 5
    assert (
        other
        == """CREATE TABLE films (
    code        char(5) CONSTRAINT firstkey PRIMARY KEY,
    title       varchar(40) NOT NULL,
    did         integer NOT NULL,
    date_prod   date,
    kind        varchar(10),
    len         interval hour to minute
);
SELECT kind, sum(len) AS total FROM films GROUP BY kind;
"""
    )


def test_roles():
    roles, _ = extract_roles("CREATE ROLE admin;")
    role = roles["admin"]

    assert role.name == "admin"

    roles, _ = extract_roles("CREATE ROLE jonathan LOGIN;")
    role = roles["jonathan"]

    assert role.name == "jonathan"
    assert role.login == "LOGIN"

    roles, _ = extract_roles(
        "CREATE ROLE george SUPERUSER CREATEDB INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION LIMIT 3;"
    )
    role = roles["george"]

    assert role.name == "george"
    assert role.createdb == "CREATEDB"
    assert role.inherit == "INHERIT"
    assert role.login == "LOGIN"
    assert role.replication == "REPLICATION"
    assert role.bypassrls == "BYPASSRLS"
    assert role.connection_limit == 3

    roles, _ = extract_roles("CREATE ROLE davide WITH PASSWORD 'jw8s0F4';")
    role = roles["davide"]

    assert role.name == "davide"

    roles, _ = extract_roles("CREATE ROLE davide WITH PASSWORD '';")
    role = roles["davide"]

    assert role.name == "davide"

    roles, _ = extract_roles("CREATE ROLE davide WITH PASSWORD NULL;")
    role = roles["davide"]

    assert role.name == "davide"

    roles, _ = extract_roles("CREATE ROLE davide2 WITH ENCRYPTED PASSWORD 'jw8s0F4';")
    role = roles["davide2"]

    assert role.name == "davide2"

    roles, _ = extract_roles(
        "CREATE ROLE miriam WITH LOGIN PASSWORD 'jw8s0F4' VALID UNTIL '2005-01-01';"
    )
    role = roles["miriam"]

    assert role.name == "miriam"
    assert role.login == "LOGIN"
    assert role.valid_until == "2005-01-01"

    roles, _ = extract_roles("CREATE ROLE admin WITH CREATEDB CREATEROLE;")
    role = roles["admin"]

    assert role.name == "admin"
    assert role.createdb == "CREATEDB"
