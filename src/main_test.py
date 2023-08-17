import sys
sys.path.append("..")

import pytest
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError
from datetime import datetime
from main import wait_for_db_connection, ingest_data, group_timestamps, aggregate_detections

# Create a test database URL (assuming a local Postgres instance)
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
                    
Base = declarative_base()

# This is the test version of the Detection class
class TestDetection(Base):
    __tablename__ = "test_detections"
    __test__ = False

    id = Column(Integer, primary_key=True)
    time = Column(DateTime)
    type = Column(String)

@pytest.fixture(scope="module")
def test_engine():
    """
    Pytest fixture that creates an engine instance for the test database.
    
    :return: An instance of the SQLAlchemy engine for the test database.
    """    
    return create_engine(TEST_DATABASE_URL)

@pytest.fixture(scope="module")
def test_session(test_engine):
    """
    Pytest fixture that sets up the test database and provides a session instance for the tests.
    It ensures that the test database is correctly set up before the tests run.
    
    :param test_engine: SQLAlchemy engine instance representing the test database connection.
    :return: A session instance to interact with the test database.
    """    
    # Ensure the test database is set up
    Base.metadata.create_all(test_engine)
    SessionLocal = sessionmaker(bind=test_engine)
    session = SessionLocal()
    yield session
    session.close()

def test_wait_for_db_connection(test_engine):
    """
    Test to ensure that the `wait_for_db_connection` function successfully waits for the database connection.
    
    :param test_engine: SQLAlchemy engine instance representing the test database connection.
    """    
    assert wait_for_db_connection(test_engine) is True

def test_wait_for_db_connection_timeout(test_engine, mocker):
    """
    Test to ensure that the `wait_for_db_connection` function times out after the specified number of retries.
    The test uses a mock to force an OperationalError to be raised on each connection attempt.
    
    :param test_engine: SQLAlchemy engine instance representing the test database connection.
    :param mocker: pytest-mock plugin used to mock objects/functions.
    """    
    # Mock the engine.connect method to always raise an OperationalError
    mocker.patch.object(test_engine, "connect", side_effect=OperationalError("mocked error", None, None))

    # Test that the wait_for_db_connection returns False after the specified number of retries
    retries = 3
    result = wait_for_db_connection(test_engine, retries=retries)
    assert result is False


def test_ingest_data(test_session):
    """
    Test to ensure that data ingestion works as expected. This function inserts a detection
    into the test database and then verifies that it has been correctly stored.
    
    :param test_session: SQLAlchemy session instance to interact with the test database.
    """    
    timestamp = datetime.now()
    detection_type = "pedestrian"
    ingest_data(test_session, TestDetection, timestamp, detection_type)
    
    detection = test_session.query(TestDetection).filter_by(time=timestamp, type=detection_type).first()
    assert detection is not None

    test_session.rollback()

def test_aggregate_detections(test_session):
    """
    Test to ensure that the `aggregate_detections` and related functions group detections correctly.
    This function inserts multiple detections into the test database and then checks the intervals
    created by the grouping functions against the expected output.
    
    :param test_session: SQLAlchemy session instance to interact with the test database.
    """    
    detections = [
        ("2023-01-01 12:00:00", "pedestrian"),
        ("2023-01-01 12:01:00", "pedestrian"),
        ("2023-01-01 12:05:00", "car"),
    ]

    for timestamp_str, detection_type in detections:
        timestamp = datetime.fromisoformat(timestamp_str)
        ingest_data(test_session, TestDetection, timestamp, detection_type)

    people_intervals = group_timestamps(test_session, TestDetection, ('pedestrian', 'bicycle'))
    assert people_intervals == [('2023-01-01 12:00:00', '2023-01-01 12:01:00')]
    
    vehicle_intervals = group_timestamps(test_session, TestDetection, ('car', 'truck', 'van'))
    assert vehicle_intervals == [('2023-01-01 12:05:00', '2023-01-01 12:05:00')]
    
    test_session.rollback()



