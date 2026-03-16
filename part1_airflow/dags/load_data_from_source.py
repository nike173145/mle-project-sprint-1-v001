import pendulum
from airflow.decorators import dag, task
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, Integer, Float, DateTime, UniqueConstraint, inspect
from utils.messages import send_telegram_success_message, send_telegram_failure_message







@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    default_args={
        'on_success_callback': send_telegram_success_message,
        'on_failure_callback': send_telegram_failure_message
    }
)
def load_data_from_source():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    table_name = "joined_data_housing"
    @task()
    def create_table() -> None:
        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, DateTime, UniqueConstraint, inspect, Boolean, text

        

        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()


        joined_data_housing = Table(
            table_name,
            metadata,

            Column("id", Integer, primary_key=True, autoincrement=True),

            Column("flat_id", Integer, nullable=False),
            Column("building_id", Integer, nullable=False),
            Column("floor", Integer),
            Column("kitchen_area", Float),
            Column("living_area", Float),
            Column("rooms", Integer),
            Column("is_apartment", Integer),
            Column("total_area", Float),
            Column("price", Float),

            Column("build_year", Integer),
            Column("building_type_int", Integer),
            Column("latitude", Float),
            Column("longitude", Float),
            Column("ceiling_height", Float),
            Column("flats_count", Integer),
            Column("floors_total", Integer),
            Column("has_elevator", Integer),

            UniqueConstraint("flat_id", name="unique_flat_constraint"),
        )

        inspector = inspect(engine)

        if inspector.has_table(table_name):
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE {table_name}"))
        
        metadata.create_all(engine)

    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()

        sql = """
        SELECT
            f.id AS flat_id,
            f.building_id,
            f.floor,
            f.kitchen_area,
            f.living_area,
            f.rooms,
            f.is_apartment,
            f.studio,
            f.total_area,
            f.price,

            b.build_year,
            b.building_type_int,
            b.latitude,
            b.longitude,
            b.ceiling_height,
            b.flats_count,
            b.floors_total,
            b.has_elevator

        FROM flats AS f
        JOIN buildings AS b
            ON b.id = f.building_id
        """

        data = pd.read_sql(sql, conn)
        conn.close()

        return data

    @task()
    def transform(data: pd.DataFrame):
        data.drop(columns=["studio"], inplace=True) 
        cols = [
            "is_apartment",
            "has_elevator"
        ]

        data[cols] = data[cols].astype(int)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        data.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

    table = create_table()
    data = extract()
    table >> data
    transformed_data = transform(data)
    load(transformed_data)
load_data_from_source()