CREATE TABLE nodes (
	  id               BIGINT PRIMARY KEY,
	  age              INTEGER,
	  public           INTEGER,
	  completion_pct   INTEGER,
	  gender           INTEGER,
	  region           TEXT,
	  last_login       TEXT,
	  registration     TEXT,
	  education        TEXT,
	  smoking          TEXT,
	  alcohol          TEXT
);

CREATE TABLE edges (
	start_id	BIGINT NOT NULL,
	end_id		BIGINT NOT NULL
);

CREATE INDEX edges_start_idx ON edges (start_id);
CREATE INDEX edges_start_end_idx ON edges (start_id, end_id);
