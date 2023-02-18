from pathlib import Path
import os
import dotenv
import boto3

from prep.scraper import BuildingApprovalsScraper
import prep.model as model


# Get root dir
BASE_DIR = Path(__file__).resolve().parent

# Load environment file
dotenv_file = os.path.join(BASE_DIR, ".env")
dotenv.load_dotenv(dotenv_file, override=True)


if __name__ == "__main__":

    # Preprocessing
    ba_scraper = BuildingApprovalsScraper()

    if os.path.exists(ba_scraper.dest_path):
        print("Building approvals data already downloaded.")

    else:
        ba_scraper.execute()

    # Access database
    db = boto3.resource(
        service_name="dynamodb",
        endpoint_url=os.environ.get("DB_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    db_tables = [table.table_name for table in db.tables.all()]

    # Access tables
    model_ba = model.BuildingApprovals(db)

    # Delete existing tables
    if model_ba.table_name in db_tables:
        model_ba.delete_table()

    # Create empty tables from model.py
    model_ba.create_table()
    model_ba.__repr__()
