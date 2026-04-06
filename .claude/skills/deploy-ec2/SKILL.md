---
name: deploy-ec2
description: Deploy EC2 instances defined in ci/infra/cloud.py using the Praktika infrastructure CLI.
argument-hint: []
disable-model-invocation: false
allowed-tools: Bash(python3 -m ci.praktika:*)
---

# Deploy EC2 Instances Skill

Deploy all EC2 instances defined in `ci/infra/cloud.py` using the Praktika infrastructure framework.

## Process

Run from the repository root:

```bash
python3 -m ci.praktika infrastructure --deploy --only EC2Instance
```

Report the output to the user.
