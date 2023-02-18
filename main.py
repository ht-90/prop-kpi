from pathlib import Path
import os
import dotenv

from prep.scraper import BuildingApprovalsScraper


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
