import sqlalchemy as sa
from sqlalchemy import create_engine, Column, Integer, String, DateTime, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import time
import os
from datetime import datetime

DATABASE_URL = os.environ.get("DATABASE_URL")
Base = declarative_base()

class BaseDetection(Base):
    __abstract__ = True  # This ensures SQLAlchemy doesn't treat BaseDetection as a table

    id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    type = Column(String)

class Detection(BaseDetection):
    __tablename__ = "detections"


def wait_for_db_connection(engine, retries=5, delay=5):
    """
    Wait for the database connection to become available.
    
    :param engine: SQLAlchemy engine instance
    :param retries: Number of times to attempt a connection before giving up.
    :param delay: Delay in seconds between connection attempts.
    :return: True if connected successfully, False otherwise.
    """
    for attempt in range(retries):
        try:
            with engine.connect():
                return True
        except sa.exc.OperationalError as e:
            if attempt < retries - 1:  # i.e., if it's not the last attempt
                print(f"Attempt {attempt + 1} failed. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to connect after {retries} attempts!")
                return False


def database_connection():
    """
    Establishes and returns a database session.
    
    :return: SQLAlchemy session instance to interact with the database.
    :raises Exception: If unable to establish a connection to the database.
    """
    engine = create_engine(DATABASE_URL)
    if not wait_for_db_connection(engine):
        raise Exception("Could not establish connection to the database.")
    
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()

def ingest_data(session, DetectionClass, timestamp: datetime, detection_type: str):
    """
    Ingest new data (timestamp and detection type) into the database and prints alerts based on data conditions.
    
    :param session: SQLAlchemy session instance.
    :param DetectionClass: The database model/class used to store detections.
    :param timestamp: DateTime object representing the time of detection.
    :param detection_type: String indicating the type of detection (e.g. 'pedestrian', 'bicycle').
    """
    detection = DetectionClass(time=timestamp, type=detection_type)
    session.add(detection)
    session.flush()  # commit the current object, making it available for query without committing the whole session.

    # Query for the last 5 records where the type is either 'pedestrian' or 'bicycle'.
    type_conditions = DetectionClass.type.in_(['pedestrian', 'bicycle'])
    last_5_detections = session.query(DetectionClass).filter(type_conditions).order_by(DetectionClass.time.desc()).limit(5).all()

    # Check if the 5 records are consecutive.
    if len(last_5_detections) == 5:
        intervals_are_consecutive = True
        for i in range(1, len(last_5_detections)):
            if (last_5_detections[i - 1].time - last_5_detections[i].time).seconds > 60:  # Assuming 60 seconds is the interval.
                intervals_are_consecutive = False
                break

        if intervals_are_consecutive:
            print("\033[91m ALERT: A person has been detected in 5 consecutive intervals. \033[0m")


# def ingest_data(session, DetectionClass, timestamp: datetime, detection_type: str):
#     """
#     Ingest new data (timestamp and detection type) into the database and prints alerts based on data conditions.
    
#     :param session: SQLAlchemy session instance.
#     :param DetectionClass: The database model/class used to store detections.
#     :param timestamp: DateTime object representing the time of detection.
#     :param detection_type: String indicating the type of detection (e.g. 'pedestrian', 'bicycle').
#     """
#     detection = DetectionClass(time=timestamp, type=detection_type)
#     session.add(detection)

#     # Set the type conditions
#     type_conditions = DetectionClass.type.in_(['pedestrian', 'bicycle'])

#     # Set the time condition
#     time_condition = DetectionClass.time <= timestamp

#     # Query, filter, and count
#     count = session.query(DetectionClass).filter(type_conditions, time_condition).count()
#     print(f"Count: {count}")
#     if count % 5 == 0:
#         print("\033[91m ALERT: A person has been detected in 5 consecutive intervals. \033[0m")

def group_timestamps(session, DetectionClass, types: tuple) -> list:
    """
    Groups timestamps based on a given type and returns intervals when detections of that type occurred.
    
    :param session: SQLAlchemy session instance.
    :param DetectionClass: The database model/class used to store detections.
    :param types: A tuple containing detection types (e.g. ('pedestrian', 'bicycle')).
    :return: A list of timestamp intervals as tuple pairs (start_time, end_time).
    """
    timestamps = session.query(DetectionClass.time).filter(DetectionClass.type.in_(types)).order_by(DetectionClass.time).all()

    timestamps = [row[0] for row in timestamps]

    intervals = []
    if timestamps:
        start = timestamps[0]
        end = timestamps[0]

        for ts in timestamps[1:]:
            if (ts - end).seconds <= 60:  # 1 minute
                end = ts
            else:
                intervals.append((str(start), str(end)))
                start = ts
                end = ts
        intervals.append((str(start), str(end)))
    return intervals

def aggregate_detections(session, DetectionClass):
    """
    Aggregates detections into 'people' and 'vehicles' based on their type and returns timestamp intervals.
    
    :param session: SQLAlchemy session instance.
    :param DetectionClass: The database model/class used to store detections.
    :return: Dictionary containing 'people' and 'vehicles' as keys and their respective timestamp intervals as values.
    """    
    return {
        "people": group_timestamps(session, DetectionClass, ('pedestrian', 'bicycle')),
        "vehicles": group_timestamps(session, DetectionClass, ('car', 'truck', 'van'))
    }

def main(DetectionClass=Detection):    # default to using the production Detection class
    session = database_connection()

    detections = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    print("Ingesting data...")
    for timestamp_str, detection_type in detections:
        timestamp = datetime.fromisoformat(timestamp_str)
        ingest_data(session, DetectionClass, timestamp, detection_type)

    session.commit()

    print("Aggregating results...")
    aggregate_results = aggregate_detections(session, DetectionClass)
    print(aggregate_results)
    print("Done!")

if __name__ == "__main__":
    main()
