from sys import argv
from google.ads.googleads.client import GoogleAdsClient
from datetime import datetime
client = GoogleAdsClient.load_from_storage("google-ads.yaml")

def validate_args(args):
    if len(args) != 2:
        print("Usage: create_test_account.py <custormer account id>")
        sys.exit()

def parse_customer_id(customer_id):
    return customer_id.replace("-", "")

def main(client, manager_customer_id):
    pass

if __name__ == "__main__":
    validate_args(argv)
    customer_id = parse_customer_id(argv[1])
    main(client, customer_id)