from typing import Generator, List, Union


from sqlalchemy import create_engine
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from more_itertools import chunked
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine import Engine
from sqlmodel import SQLModel, Session, inspect, Table
from tqdm import tqdm

from app.core.config import settings


engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator:
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()


def upsert_database(
    data: List,
    table: Union[str, SQLModel],
    schema: str,
    engine: Engine = engine,
    batch_size: int = 200,
) -> None:
    """Upsert data to postgre datatable

    Args:
        data (List): List of dictionaries or objects containing data to be upserted.
        engine (Engine): SQLAlchemy engine object to the database.
        table (str | SQLModel): Name of the table or SQLModel class to upsert data into.
        schema (str, optional): Schema name. Defaults to 'public'.
        batch_size (int, optional): Number of records per batch. Defaults to 1000.

    Raises:
        Exception: If an error occurs during the upsert operation.
    """
    if not data:
        return

    target_table = table
    if isinstance(table, str):
        base = automap_base()
        with engine.connect() as connection:
            base.prepare(connection, reflect=True, schema=schema)
            target_table = Table(
                table, base.metadata, schema=schema, autoload_with=connection
            )

    primary_keys = [key.name for key in inspect(target_table).primary_key]
    chunks = list(chunked(data, batch_size))
    table_name = table if isinstance(table, str) else table.__tablename__

    with Session(engine) as session:
        for idx, chunk in enumerate(
            tqdm(chunks, desc=f"Upserting {len(data)} records to {schema}.{table_name}")
        ):
            try:
                for record in chunk:
                    if not all(pk in record for pk in primary_keys):
                        print(f"Skipping record missing primary key fields: {record}")
                        continue

                    pk_conditions = [
                        getattr(target_table.c, pk) == record[pk] for pk in primary_keys
                    ]
                    exists_query = (
                        sa.select(sa.literal(1))
                        .select_from(target_table)
                        .where(sa.and_(*pk_conditions))
                    )
                    exists = session.exec(exists_query).scalar() is not None

                    if exists:
                        update_values = {
                            k: v
                            for k, v in record.items()
                            if k not in primary_keys and v is not None
                        }
                        if update_values:
                            update_stmt = (
                                sa.update(target_table)
                                .where(sa.and_(*pk_conditions))
                                .values(**update_values)
                            )
                            session.exec(update_stmt)
                    else:
                        insert_stmt = sa.insert(target_table).values(record)
                        session.exec(insert_stmt)

                session.commit()
            except Exception:
                print(
                    f"Error upserting data to {schema}.{table_name} at chunk {idx}",
                    exc_info=True,
                )
                session.rollback()
                raise
