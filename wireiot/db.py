import os

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def init():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(370029)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                id text PRIMARY KEY, token_hash text NOT NULL,
                threshold numeric(12,3) NOT NULL, timeout integer NOT NULL,
                last_received timestamptz NOT NULL DEFAULT now());
            CREATE TABLE IF NOT EXISTS events (
                device text REFERENCES devices(id), id uuid NOT NULL,
                happened_at timestamptz NOT NULL, received_at timestamptz NOT NULL,
                bucket timestamptz NOT NULL, value numeric(12,3) NOT NULL,
                included boolean NOT NULL, processed boolean NOT NULL DEFAULT false,
                PRIMARY KEY(device,id));
            CREATE INDEX IF NOT EXISTS pending_events ON events(received_at) WHERE NOT processed;
            CREATE TABLE IF NOT EXISTS windows (
                device text REFERENCES devices(id), bucket timestamptz NOT NULL,
                count bigint NOT NULL, total numeric NOT NULL, minimum numeric NOT NULL, maximum numeric NOT NULL,
                revision bigint NOT NULL, active boolean NOT NULL DEFAULT false,
                PRIMARY KEY(device,bucket));
            CREATE TABLE IF NOT EXISTS transitions (
                device text NOT NULL, bucket timestamptz NOT NULL, revision bigint NOT NULL,
                active boolean NOT NULL, mean numeric NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY(device,bucket,revision));
            ALTER TABLE devices ADD COLUMN IF NOT EXISTS rule_revision integer NOT NULL DEFAULT 1;
            ALTER TABLE devices ADD COLUMN IF NOT EXISTS key_revision integer NOT NULL DEFAULT 1;
            ALTER TABLE devices ADD COLUMN IF NOT EXISTS served_at timestamptz NOT NULL DEFAULT '-infinity';
            ALTER TABLE windows ADD COLUMN IF NOT EXISTS rule_version integer NOT NULL DEFAULT 1;
            ALTER TABLE windows ADD COLUMN IF NOT EXISTS dirty boolean NOT NULL DEFAULT false;
            ALTER TABLE transitions ADD COLUMN IF NOT EXISTS rule_version integer NOT NULL DEFAULT 1;
            ALTER TABLE transitions ADD COLUMN IF NOT EXISTS reason text NOT NULL DEFAULT 'telemetry';
            CREATE TABLE IF NOT EXISTS rules (
                device text REFERENCES devices(id),version integer NOT NULL,effective_at timestamptz NOT NULL,
                threshold numeric(12,3) NOT NULL,aggregate text NOT NULL,minimum_count integer NOT NULL,
                PRIMARY KEY(device,version));
            INSERT INTO rules SELECT id,1,'-infinity',threshold,'mean',1 FROM devices ON CONFLICT DO NOTHING;
            CREATE INDEX IF NOT EXISTS rule_lookup ON rules(device,effective_at DESC,version DESC);
            CREATE TABLE IF NOT EXISTS key_changes (
                device text NOT NULL,revision integer NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY(device,revision));
        """)
