#!/usr/bin/python

import boto3
import json

session = boto3.Session()
res = {}
for region in ["us-east-2", "us-west-2"]:
    client = session.client("ecr", region_name=region)
    token = client.get_authorization_token()["authorizationData"][0]["authorizationToken"]
    res[region] = token

print(json.dumps(res))
