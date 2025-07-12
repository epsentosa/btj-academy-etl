import csv
import logging

from sqlalchemy.orm.session import Session

from pipeline.db.table import ETLConfig
from pipeline.db.table import TaxiZoneLookup


def add_default_sftp_config(session: Session) -> None:
    count = session.query(ETLConfig).count()
    if count > 0:
        return
    etl_config = ETLConfig(
        name="NYCTrip",
        method="SFTP",
        host="localhost",
        port="2222",
        username="btj-academy",
        password="btj-academy",
        path="/upload",
    )
    session.add(etl_config)
    session.commit()


def add_taxi_zone_lookup(session: Session) -> None:
    count = session.query(TaxiZoneLookup).count()
    if count > 0:
        return
    with open("pipeline/files/taxi_zone_lookup.csv", "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            zone_lookup = TaxiZoneLookup(
                location_id=int(row["LocationID"]),
                borough=row["Borough"],
                zone=row["Zone"],
                service_zone=row["service_zone"],
            )
            session.add(zone_lookup)
        session.commit()


def post_migration(session: Session) -> None:
    logger = logging.getLogger("alembic.post_migration")
    logger.info("doing post migration")
    add_default_sftp_config(session)
    add_taxi_zone_lookup(session)
    logger.info("post migration completed")
