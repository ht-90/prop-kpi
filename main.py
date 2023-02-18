from prep.scraper import BuildingApprovalsScraper

if __name__ == "__main__":

    # Preprocessing
    ba_scraper = BuildingApprovalsScraper()

    if os.path.exists(ba_scraper.dest_path):
        print("Building approvals data already downloaded.")

    else:
        ba_scraper.execute()
