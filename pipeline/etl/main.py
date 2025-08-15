import argparse

from pipeline.etl.config import ETLType
from pipeline.worker_app import app


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name")
    parser_run = subparsers.add_parser("run")
    parser_run.add_argument(
        "mode",
        choices=[
            "start-etl",
        ],
    )
    parser_run.add_argument(
        "config",
        choices=[
            "nyc_trip",
            "chicago_trip",
        ],
    )

    args, unknownargs = parser.parse_known_args()

    if args.subparser_name == "run" and args.mode == "start-etl":
        if args.config == "nyc_trip":
            etl_type: ETLType = ETLType("NYCTrip")
        elif args.config == "chicago_trip":
            etl_type = ETLType("ChicagoTrip")

        app.signature("pipeline.etl.etl.remote_check").apply_async(
            kwargs={"etl_type": etl_type}, queue="etl"
        )


if __name__ == "__main__":
    main()
