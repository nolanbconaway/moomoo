"""Storage for embeddings."""
from pathlib import Path
from typing import List
from psycopg import Connection

from .scorer import EmbeddingResult

EXTENSIONS = set([".mp3", ".flac"])

DDL = [
    """
    create table {schema}.{table} (
        filepath varchar not null primary key
        , success boolean not null
        , fail_reason varchar
        , duration_seconds float
        , embedding vector(1024)
        , insert_ts_utc timestamp with time zone default current_timestamp not null
    )
    """,
]


def insert_embedding(
    conn: Connection,
    filepath: Path,
    embedding: EmbeddingResult,
    schema: str,
    table: str,
):
    """Upsert an embedding into the database.

    Args:
        conn: psycopg connection
        base_path: Path to the base of the music library.
        local_path: Path to the file relative to the base path.
        schema: Schema to insert into.
        table: Table to insert into.
    """
    insert_sql = """
        insert into {schema}.{table} (
            filepath, success, fail_reason, duration_seconds, embedding
        ) 
        values (
            %(filepath)s
            , %(success)s
            , %(fail_reason)s
            , %(duration_seconds)s
            , %(embedding)s
        )
        on conflict (filepath) do update set
            success = excluded.success
            , fail_reason = excluded.fail_reason
            , duration_seconds = excluded.duration_seconds
            , embedding = excluded.embedding
            , insert_ts_utc = current_timestamp

    """
    with conn.cursor() as cur:
        cur.execute(
            insert_sql.format(schema=schema, table=table),
            {
                "filepath": str(filepath),
                "success": embedding.success,
                "fail_reason": embedding.fail_reason,
                "duration_seconds": embedding.duration_seconds,
                "embedding": (
                    embedding.embedding.tolist() if embedding.success else None
                ),
            },
        )
    conn.commit()


def list_audio_files(src_dir: Path) -> List[Path]:
    """List all audio files in the directories."""
    return [
        p
        for p in src_dir.rglob("**/*")
        if p.is_file() and p.suffix.lower() in EXTENSIONS
    ]
