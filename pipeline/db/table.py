import sqlalchemy_utils as su
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


su.force_auto_coercion()  # type: ignore


class ETLConfig(Base):
    __tablename__ = "etl_config"
    __table_args__ = ((UniqueConstraint("name", "host")),)
    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String)
    method = Column(String)
    host = Column(String)
    port = Column(Integer)
    username = Column(String)
    password = Column(String)
    path = Column(String)


class TaxiZoneLookup(Base):
    __tablename__ = "taxi_zone_lookup"
    location_id = Column(Integer, autoincrement=True, primary_key=True)
    borough = Column(String)
    zone = Column(String)
    service_zone = Column(String)


class NycTrip(Base):
    __tablename__ = "nyc_trip"
    __table_args__ = (
        Index(
            "ix_nyc_trip_time",
            "pickup_time",
            "dropoff_time",
        ),
    )
    vendor = Column(String, nullable=False)
    # The primary_key kwargs is a lie, the table doesn't actually have primary key, only for SQLAlchemy requirement
    pickup_time = Column(DateTime, primary_key=True)
    dropoff_time = Column(DateTime)
    passenger_count = Column(Integer, nullable=False)
    trip_distance = Column(Float, nullable=False)
    pu_location_region = Column(String, nullable=False)
    pu_location_zone = Column(String, nullable=False)
    do_location_regin = Column(String, nullable=False)
    do_location_zone = Column(String, nullable=False)
    payment_type = Column(String, nullable=False)
    fare_amount = Column(Float)
    extra = Column(Float)
    mta_tax = Column(Float)
    tip_amount = Column(Float)
    tolls_amount = Column(Float)
    improvement_surcharge = Column(Float)
    total_amount = Column(Float)
    congestion_surcharge = Column(Float)
    airport_fee = Column(Float)


class ETLStateSummary(Base):
    __tablename__ = "etl_state_summary"
    id = Column(Integer, autoincrement=True, primary_key=True)
    config_type = Column(String)
    state = Column(String)
    host = Column(String)
    file_datetime = Column(DateTime, nullable=False)
    latest_modified_time = Column(DateTime, nullable=False)
    queue_on = Column(DateTime, nullable=False)
    finished_on = Column(DateTime)
    duration = Column(Integer)
    record_count = Column(Integer)


class ChicagoTrip(Base):
    __tablename__ = "chicago_trip"
    __table_args__ = (
        Index(
            "ix_chicago_trip_time",
            "trip_start",
            "trip_end",
        ),
    )
    # The primary_key kwargs is a lie, the table doesn't actually have primary key, only for SQLAlchemy requirement
    trip_start = Column(DateTime, primary_key=True)
    trip_end = Column(DateTime)
    trip_miles = Column(Float)
    fare = Column(Float)
    tips = Column(Float)
    tolls = Column(Float)
    extras = Column(Float)
    trip_total = Column(Float)
    payment_type = Column(String)
    company = Column(String)
    pickup_centroid_latitude = Column(String)
    pickup_centroid_longitude = Column(String)
    dropoff_centroid_latitude = Column(String)
    dropoff_centroid_longitude = Column(String)
