import json
import logging
import boto3
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
cw_client = boto3.client("cloudwatch")

NAMESPACE = "ShopStream/Ingress"

REQUIRED_FIELDS = {

    "page_view": [
        "user_id",
        "session_id",
        "timestamp",
        "page_url",
        "page_type",
        "time_on_page_seconds",
        "device_type",
        "country"
    ],

    "click": [
        "user_id",
        "session_id",
        "timestamp",
        "element_id",
        "element_type",
        "page_url",
        "x_position",
        "y_position"
    ],

    "search": [
        "user_id",
        "session_id",
        "timestamp",
        "query",
        "results_count"
    ],

    "product_view": [
        "user_id",
        "session_id",
        "timestamp",
        "product_id",
        "category",
        "price",
        "time_on_page_seconds"
    ],

    "cart_event": [
        "user_id",
        "session_id",
        "timestamp",
        "product_id",
        "action"
    ]
}

VALID_ACTIONS = {"add", "remove"}

VALID_DEVICES = {
    "mobile",
    "desktop",
    "tablet"
}

VALID_PAGE_TYPES = {
    "home",
    "category",
    "product",
    "cart",
    "checkout"
}


def put_metric(metric_name, value, unit="Count"):

    try:

        cw_client.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit
                }
            ]
        )

    except Exception as e:

        logger.warning(
            f"CloudWatch metric error: {str(e)}"
        )


def validate_event(record):

    event_type = record.get("event_type")

    if event_type not in REQUIRED_FIELDS:

        raise ValueError(
            f"Unknown event type: {event_type}"
        )

    missing_fields = [
        field
        for field in REQUIRED_FIELDS[event_type]
        if field not in record
    ]

    if missing_fields:

        raise ValueError(
            f"Missing fields: {missing_fields}"
        )

    # page_view validations

    if event_type == "page_view":

        if not isinstance(
            record.get("time_on_page_seconds"),
            (int, float)
        ):

            raise ValueError(
                "Invalid time_on_page_seconds"
            )

        if record["time_on_page_seconds"] < 0:

            raise ValueError(
                "Negative time_on_page_seconds"
            )

        if record.get("device_type") not in VALID_DEVICES:

            raise ValueError(
                f"Invalid device_type: {record.get('device_type')}"
            )

        if record.get("page_type") not in VALID_PAGE_TYPES:

            raise ValueError(
                f"Invalid page_type: {record.get('page_type')}"
            )

    # cart_event validations

    if event_type == "cart_event":

        if record.get("action") not in VALID_ACTIONS:

            raise ValueError(
                f"Invalid action: {record.get('action')}"
            )

    # product_view validations

    if event_type == "product_view":

        if not isinstance(
            record.get("price"),
            (int, float)
        ):

            raise ValueError(
                "Invalid price type"
            )

        if float(record["price"]) < 0:

            raise ValueError(
                "Negative price"
            )


def process_s3_object(bucket, key, size):

    logger.info(
        f"Processing file: s3://{bucket}/{key}"
    )

    try:

        response = s3_client.get_object(
            Bucket=bucket,
            Key=key
        )

        body = response["Body"].read().decode("utf-8")

    except Exception as e:

        logger.error(
            f"Error downloading object {key}: {str(e)}"
        )

        raise e

    errors = []

    for line_number, line_content in enumerate(
        body.splitlines(),
        1
    ):

        line_content = line_content.strip()

        if not line_content:
            continue

        try:

            event_record = json.loads(line_content)

            validate_event(event_record)

        except json.JSONDecodeError as e:

            errors.append(
                f"Line {line_number}: Invalid JSON - {str(e)}"
            )

            break

        except ValueError as e:

            errors.append(
                f"Line {line_number}: Validation error - {str(e)}"
            )

            break

    # INVALID FILE

    if errors:

        error_message = " | ".join(errors)[:256]

        quarantine_key = f"quarantine/{key}"

        logger.warning(
            f"Invalid file. Moving to quarantine: {quarantine_key}"
        )

        # copy file to quarantine

        s3_client.copy_object(
            Bucket=bucket,
            CopySource={
                "Bucket": bucket,
                "Key": key
            },
            Key=quarantine_key,
            Metadata={
                "validation_error": error_message,
                "original_key": key
            },
            MetadataDirective="REPLACE"
        )

        # delete original file

        s3_client.delete_object(
            Bucket=bucket,
            Key=key
        )

        put_metric("InvalidFiles", 1)

        return False

    # VALID FILE

    logger.info(
        f"Valid file: {key}"
    )

    put_metric("FilesProcessed", 1)

    put_metric(
        "BytesProcessed",
        size,
        unit="Bytes"
    )

    return True


def lambda_handler(event, context):

    logger.info(
        f"Received event: {json.dumps(event)}"
    )

    results = []

    for record in event.get("Records", []):

        try:

            bucket_name = record["s3"]["bucket"]["name"]

            object_key = unquote_plus(
                record["s3"]["object"]["key"]
            )

            object_size = record["s3"]["object"].get("size", 0)

            # avoid infinite loop

            if object_key.startswith("quarantine/"):
                continue

            is_valid = process_s3_object(
                bucket_name,
                object_key,
                object_size
            )

            results.append({
                "key": object_key,
                "valid": is_valid
            })

        except Exception as e:

            logger.error(
                f"Unexpected error: {str(e)}"
            )

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }