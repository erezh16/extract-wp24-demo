#!/bin/bash

# Load the environment variables
source .env

# Store inference data on SkyStore S3-proxy connected to eu-central-1
python -m client.produce reduced_tronchetto_array.pt

echo Go back to notebook - notification should have been received
