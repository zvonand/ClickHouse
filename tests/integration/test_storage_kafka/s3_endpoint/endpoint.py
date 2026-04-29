from bottle import route, run

### AWS zone API mock


@route("/latest/api/token", ["PUT"])
def api_token():
    return "manually_crafted_token"


@route("/latest/meta-data/placement/availability-zone-id")
def placement_availability_zone_id():
    return "euc1-az2"


@route("/latest/meta-data/placement/availability-zone")
def placement_availability_zone():
    return "eu-central-1a"


@route("/computeMetadata/v1/instance/zone")
def gcp_zone():
    return "projects/123456789/zones/europe-central2-a"


@route("/ping")
def ping():
    return "OK"


run(host="0.0.0.0", port=8080)
