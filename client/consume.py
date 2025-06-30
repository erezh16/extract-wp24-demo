import os
from pprint import pprint

import boto3
from botocore.config import Config

from catalogue.consumer import DataConsumer
from nuvla.api import Api as Nuvla

S3_BUCKET = os.getenv("S3_BUCKET")

NUVLA_ENDPOINT = os.getenv("NUVLA_ENDPOINT", "https://nuvla.io")
NUVLA_KEY = os.getenv("NUVLA_KEY", "")
NUVLA_KEY_SECRET = os.getenv("NUVLA_SECRET", "")

# MQTT Configuration
TOPIC = os.getenv("MQTT_TOPIC", "test-topic")
MQTT_BROKER: str = os.getenv("MQTT_BROKER", "91.134.104.104")
MQTT_PORT: int = int(os.getenv("MQTT_PORT", 1883))

# Additional S3 Configuration
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY_ID_SITE_2", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_ACCESS_KEY_SITE_2")
S3_ENDPOINT_SITE_2 = os.getenv("S3_ENDPOINT_SITE_2")

def main():
    nuvla: Nuvla = Nuvla(endpoint=NUVLA_ENDPOINT, insecure=False)
    resp = nuvla.login_apikey(NUVLA_KEY, NUVLA_KEY_SECRET)
    if not resp or resp.status_code != 201:
        pprint("Failed to login to Nuvla. Please check your credentials.")
        pprint(resp.content)
        exit(1)
    consumer: DataConsumer = DataConsumer(
        nuvla=nuvla,
        topic=TOPIC,
        host=MQTT_BROKER,
        port=MQTT_PORT
    )

    consumer.listen()

    while True:
        try:
            msg = consumer.link_queue.get()
            bucket, file_name, download_link = get_relevant_data(msg)
            if not S3_ACCESS_KEY or not S3_SECRET_KEY or not S3_ENDPOINT_SITE_2:
                pprint("S3 credentials or endpoint not set. Skipping download.")
                continue
            download_link_site_2 = generate_url(S3_ENDPOINT_SITE_2, S3_ACCESS_KEY, S3_SECRET_KEY, bucket, file_name)
            print(f"Download link 2: {download_link_site_2}")
        except KeyboardInterrupt:
            print("Exiting consumer...")
            break

        except Exception as e:
            print(f"Couldn't find the link: {e}")

def get_relevant_data(msg: dict) -> [str, str, str]:
    bucket = msg.get("data-object", {}).get("bucket", "")
    file_name = msg.get("data-object", {}).get("object", "")
    uri = msg.get("link", {}).get("uri", "")
    print(f"Bucket: {bucket}")
    print(f"File Name: {file_name}")
    print(f"Download Link: {uri}")
    return bucket, file_name, uri

def generate_url(endpoint_url, access_key, secret_key, bucket_name, object_key, expiration=3600):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )

    url = s3_client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_key
        },
        ExpiresIn=expiration
    )

    return url

if __name__ == "__main__":
    pprint("Starting Data Consumer...")
    pprint("Configuration:")
    pprint(f"NUVLA Endpoint: {NUVLA_ENDPOINT}")
    pprint(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}/{TOPIC}")
    pprint(f"Using Nuvla Key: {NUVLA_KEY}")
    pprint(f"Using Nuvla Secret: {NUVLA_KEY_SECRET}")

    if not NUVLA_KEY or not NUVLA_KEY_SECRET:
        print("Please set NUVLA_KEY and NUVLA_KEY_SECRET environment variables.")
        exit(1)
    main()
