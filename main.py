from prep.scraper import BuildingApprovalsScraper

if __name__ == "__main__":

    # Preprocessing
    ba_scraper = BuildingApprovalsScraper()
    ba_scraper.execute()
