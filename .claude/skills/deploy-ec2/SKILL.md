---
name: deploy-ec2
description: Deploy EC2 instances defined in ci/infra/cloud.py using the Praktika infrastructure CLI.
argument-hint: []
disable-model-invocation: false
allowed-tools: 
---

# Deploy EC2 Instances Skill

Deploy all EC2 instances defined in `ci/infra/cloud.py` using the Praktika infrastructure framework.

## Process

Show the user the following command and ask them to run it in a separate terminal (not here), to avoid exposing AWS credentials to the AI:

```bash
python3 -m ci.praktika infrastructure --deploy --only EC2Instance
```

Do not run the command yourself.
