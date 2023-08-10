import sqlalchemy as sa


def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
    conn = engine.connect()
    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS detections "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


def ingest_data(conn: sa.Connection, timestamp: str, detection_type: str):
    ...


def aggregate_detections(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    return {
        "people": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:04:00", "2023-08-10T10:05:00"),
        ],
        "vehicles": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:05:00", "2023-08-10T10:07:00"),
        ],
    }


def main():
    conn = database_connection()

    ingest_data(conn, "2023-08-10T10:00:00", "car")

    detections = aggregate_detections(conn)

    print(detections["people"])
    print(detections["vehicles"])


if __name__ == "__main__":
    main()
