from os.path import isdir, isfile
from posix import remove
from sys import argv
from google.ads.googleads.client import GoogleAdsClient
from datetime import datetime
import sys
import os
import shutil
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from google.cloud import storage
from google.cloud import secretmanager
import functions_framework
from markupsafe import escape

SECRET_ID = os.environ["SECRET_ID"]
BUCKET_NAME = os.environ["BUCKET_NAME"]



@functions_framework.http
def main(request):

    # Create Google Ads client
    config = load_google_ads_creds()
    client = GoogleAdsClient.load_from_string(config)

    customer_id = parse_customer_id(request.args["customerId"])
    extract_leads(client, customer_id)

    return "Done"

def load_google_ads_creds():
    # Load from secretm manager
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": f"{SECRET_ID}/versions/latest"})

    payload = response.payload.data.decode("UTF-8")
    return payload

def parse_customer_id(customer_id):
    return customer_id.replace("-", "")

def setup_data_folder():
    # Prepares the data folder
    data_folder = "data"
    if os.path.isdir(data_folder):
        # Clean it
        for filename in os.listdir():
            path = os.path.join(data_folder, filename)
            if os.path.isfile(path):
                os.remove(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
    
    else:
        os.mkdir("data")

def clear_gcs_bucket(bucket_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        blob.delete()
    print(f"All contents of {bucket_name} have been deleted")

    
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )



def extract_leads(client, manager_customer_id):

    # Prepare the data folder
    setup_data_folder()

    # Setup avro writer
    schema = avro.schema.parse(open("lead_data.avsc", "rb").read())

    service = client.get_service("GoogleAdsService", version="v16")

    query = """
        SELECT 
            lead_form_submission_data.id, 
            ad_group_ad.ad.id 
        FROM lead_form_submission_data
            """

    search_request = client.get_type("SearchGoogleAdsStreamRequest")
    search_request.customer_id = manager_customer_id
    search_request.query = query
    response = service.search_stream(search_request)

    batch_counter = 0
    for batch in response:

        file_path = f"data/lead_data_{batch_counter}.avro"
        writer = writer = DataFileWriter(open(file_path, "wb"), DatumWriter(), schema)
        for row in batch.results:
            print(row)
            writer.append({
                "id": row.lead_form_submission_data.id,
                "ad_id": row.ad_group_ad.id
                })
        writer.close()
        batch_counter += 1

    # Delete old files from  GCS
    clear_gcs_bucket(os.environ["BUCKET_NAME"])

    # Upload all avro files to google cloud storage
    for filename in os.listdir("data"):
        src_path = os.path.join("data", filename)
        upload_blob(BUCKET_NAME, src_path, filename)    
