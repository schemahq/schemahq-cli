CREATE SCHEMA api;

CREATE TABLE api.repos (
  repo TEXT NOT NULL,
  UNIQUE (repo),
  PRIMARY KEY (repo)
);

CREATE TABLE api.environments (
  id SERIAL,
  repo TEXT,
  name TEXT,
  host TEXT,
  port TEXT,
  username TEXT,
  pass TEXT,
  maintenance_database TEXT,
  PRIMARY KEY (id),
  FOREIGN KEY (repo) REFERENCES api.repos (repo)
);

CREATE TABLE api.unresolved (
  repo TEXT,
  branch TEXT,
  commit_sha TEXT,
  UNIQUE (repo, branch),
  PRIMARY KEY (repo)
);

CREATE TABLE api.branches (
  repo TEXT,
  branch TEXT,
  environment INT,
  db TEXT,
  UNIQUE (repo, branch),
  FOREIGN KEY (repo) REFERENCES api.repos (repo),
  FOREIGN KEY (environment) REFERENCES api.environments (id)
);
