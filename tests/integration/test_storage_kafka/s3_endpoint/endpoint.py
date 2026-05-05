import argparse

from bottle import abort, route, run

### Metadata API mock

SERVICE = "aws"


@route("/latest/api/token", ["PUT"])
def api_token():
    if SERVICE != "aws":
        abort(404, "Not Found")
    return "manually_crafted_token"


@route("/latest/meta-data/placement/availability-zone-id")
def placement_availability_zone_id():
    if SERVICE != "aws":
        abort(404, "Not Found")
    return "euc1-az2"


@route("/latest/meta-data/placement/availability-zone")
def placement_availability_zone():
    if SERVICE != "aws":
        abort(404, "Not Found")
    abort(404, "Not Found")


@route("/computeMetadata/v1/instance/zone")
def gcp_zone():
    if SERVICE != "gcp":
        abort(404, "Not Found")
    return "projects/123456789/zones/europe-central2-a"


@route("/ping")
def ping():
    return "OK"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", choices=["aws", "gcp"], required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    SERVICE = args.service
    run(host="0.0.0.0", port=args.port)
