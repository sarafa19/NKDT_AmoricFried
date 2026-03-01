import logging
import json
import azure.functions as func
from typing import List
from pydantic import BaseModel
import os
from datetime import datetime, timezone

# storage bucket using Azure Blob (persistence across cold starts)
# the connection string is provided by the Functions runtime via
#   AzureWebJobsStorage environment variable
from azure.storage.blob import BlobServiceClient

CONTAINER_NAME = "feedbacks"

conn_str = os.getenv("AzureWebJobsStorage")
_blob_service = BlobServiceClient.from_connection_string(conn_str)

try:
    _blob_service.create_container(CONTAINER_NAME)
except Exception:
    pass  # container may already exist


def _read_blob(name: str) -> List[dict]:
    client = _blob_service.get_blob_client(CONTAINER_NAME, name)
    if not client.exists():
        return []
    data = client.download_blob().readall().decode("utf-8")
    return json.loads(data) if data else []


def _write_blob(name: str, arr: List[dict]):
    client = _blob_service.get_blob_client(CONTAINER_NAME, name)
    client.upload_blob(json.dumps(arr), overwrite=True)


# optional schema for validation; mirrors the API pusher payload
class Feedback(BaseModel):
    username: str
    feedback_date: str
    campaign_id: str
    comment: str


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('GetFeedback function processed a request. method=%s', req.method)

    method = req.method.upper()
    if method == 'GET':
        # return all stored feedbacks from blob container
        all_items = _read_blob("all.json")
        return func.HttpResponse(
            json.dumps(all_items),
            mimetype="application/json"
        )

    # POST behaviour
    if method != 'POST':
        return func.HttpResponse(
            "This endpoint only accepts GET for reading or POST requests with JSON body.",
            status_code=405
        )

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON payload", status_code=400)

    # allow either a single object or a list
    if isinstance(body, dict):
        items = [body]
    elif isinstance(body, list):
        items = body
    else:
        return func.HttpResponse("Payload must be an object or array", status_code=400)

    # validate each item (optional)
    validated = []
    for itm in items:
        try:
            fb = Feedback(**itm)
            fb_dict = fb.dict()
            # add server-side insertion timestamp for partitioning
            fb_dict["received_at"] = datetime.utcnow().isoformat()
            validated.append(fb_dict)
        except Exception as e:
            logging.error(f"Validation error: {e}")
            return func.HttpResponse(f"Invalid item: {e}", status_code=400)

    # append to blob file
    current = _read_blob("all.json")
    current.extend(validated)
    _write_blob("all.json", current)

    logging.info(f"stored total={len(current)} items")

    return func.HttpResponse(
        body=f"Received {len(validated)} feedback(s)",
        status_code=201
    )
    
