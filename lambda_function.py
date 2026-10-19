
import json
import boto3

BUCKET = "cs5224-lab2-twitter-a0262582b"
PREFIX = "output/user_counts/"

s3 = boto3.client("s3")
cached_results = None


def load_results():
    global cached_results

    if cached_results is not None:
        return cached_results

    results = {}
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.split("/")[-1].startswith("part-") or not key.endswith(".json"):
                continue

            response = s3.get_object(Bucket=BUCKET, Key=key)

            for line in response["Body"].iter_lines():
                if line:
                    record = json.loads(line)
                    results[record["user_id"]] = record

    cached_results = results
    return results


def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    results = load_results()

    if "user_id" in params:
        user_id = params["user_id"]
        user = results.get(user_id)

        if user is None:
            return make_response(404, {"error": "User not found"})

        return make_response(200, {"results": [user]})

    try:
        offset = max(0, int(params.get("offset", 0)))
        limit = max(1, min(100, int(params.get("limit", 50))))
    except ValueError:
        return make_response(400, {"error": "Invalid pagination parameters"})

    user_ids = sorted(results, key=int)
    selected = user_ids[offset:offset + limit]

    return make_response(200, {
        "total": len(user_ids),
        "offset": offset,
        "limit": limit,
        "results": [results[uid] for uid in selected]
    })


def make_response(status, data):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(data)
    }
