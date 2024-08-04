#!/bin/bash

# Function to display usage information
usage() {
	echo "Usage: $0 -f <local_file_path> -b <bucket_name> -d <destination_path> -k <service_account_key_path>"
	echo
	echo "Options:"
	echo "  -f  Path to the local CSV file"
	echo "  -b  Name of the GCS bucket"
	echo "  -d  Destination path in the bucket"
	echo "  -k  Path to the service account key JSON file"
	exit 1
}

# Parse command-line arguments
while getopts ":f:b:d:k:" opt; do
	case $opt in
	f) LOCAL_FILE_PATH="$OPTARG" ;;
	b) BUCKET_NAME="$OPTARG" ;;
	d) DESTINATION_PATH="$OPTARG" ;;
	k) SERVICE_ACCOUNT_KEY="$OPTARG" ;;
	\?)
		echo "Invalid option -$OPTARG" >&2
		usage
		;;
	:)
		echo "Option -$OPTARG requires an argument" >&2
		usage
		;;
	esac
done

# Check if all required arguments are provided
if [ -z "$LOCAL_FILE_PATH" ] || [ -z "$BUCKET_NAME" ] || [ -z "$DESTINATION_PATH" ] || [ -z "$SERVICE_ACCOUNT_KEY" ]; then
	echo "Error: Missing required arguments"
	usage
fi

# Check if the local file exists
if [ ! -f "$LOCAL_FILE_PATH" ]; then
	echo "Error: Local file does not exist: $LOCAL_FILE_PATH"
	exit 1
fi

# Check if the service account key file exists
if [ ! -f "$SERVICE_ACCOUNT_KEY" ]; then
	echo "Error: Service account key file does not exist: $SERVICE_ACCOUNT_KEY"
	exit 1
fi

# Export the path to the service account key file
export GOOGLE_APPLICATION_CREDENTIALS="$SERVICE_ACCOUNT_KEY"

# Authenticate using the service account
gcloud auth activate-service-account --key-file="$SERVICE_ACCOUNT_KEY"

# Upload the file to GCS bucket
gsutil cp "$LOCAL_FILE_PATH" "gs://$BUCKET_NAME/$DESTINATION_PATH"

# Check if the upload was successful
if [ $? -eq 0 ]; then
	echo "File successfully uploaded to gs://$BUCKET_NAME/$DESTINATION_PATH"
else
	echo "Error uploading file to GCS bucket"
	exit 1
fi
