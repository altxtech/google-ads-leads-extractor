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
    customer_service = client.get_service("CustomerService")
    customer = client.get_type("Customer")
    now = datetime.today().strftime("%Y%m%d %H:%M:%S")
    customer.descriptive_name = f"Account created with CustomerService on {now}"
    # For a list of valid currency codes and time zones see this documentation:
    # https://developers.google.com/google-ads/api/reference/data/codes-formats
    customer.currency_code = "USD"
    customer.time_zone = "America/New_York"
    # The below values are optional. For more information about URL
    # options see: https://support.google.com/google-ads/answer/6305348
    customer.tracking_url_template = "{lpurl}?device={device}"
    customer.final_url_suffix = (
        "keyword={keyword}&matchtype={matchtype}" "&adgroupid={adgroupid}"
    )

    response = customer_service.create_customer_client(
        customer_id=manager_customer_id, customer_client=customer
    )
    print(
        f'Customer created with resource name "{response.resource_name}" '
        f'under manager account with ID "{manager_customer_id}".'
    )

if __name__ == "__main__":
    validate_args(argv)
    customer_id = parse_customer_id(argv[1])
    main(client, customer_id)