#!/usr/bin/env bash
set -euo pipefail

echo "Running prepare script"
export DEBIAN_FRONTEND=noninteractive
export RUNNER_VERSION=2.285.0
export RUNNER_HOME=/home/ubuntu/actions-runner

apt-get update

apt-get install --yes --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    python3-pip \
    unzip

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update

apt-get install --yes --no-install-recommends docker-ce docker-ce-cli containerd.io

usermod -aG docker ubuntu

# enable ipv6 in containers (fixed-cidr-v6 is some random network mask)
cat <<EOT > /etc/docker/daemon.json
{
  "ipv6": true,
  "fixed-cidr-v6": "2001:db8:1::/64"
}
EOT

systemctl restart docker

pip install boto3 pygithub requests urllib3 unidiff dohq-artifactory

mkdir -p $RUNNER_HOME && cd $RUNNER_HOME

curl -O -L https://github.com/actions/runner/releases/download/v$RUNNER_VERSION/actions-runner-linux-x64-$RUNNER_VERSION.tar.gz

tar xzf ./actions-runner-linux-x64-$RUNNER_VERSION.tar.gz
rm -f ./actions-runner-linux-x64-$RUNNER_VERSION.tar.gz
./bin/installdependencies.sh

chown -R ubuntu:ubuntu $RUNNER_HOME

cd /home/ubuntu
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install

rm -rf /home/ubuntu/awscliv2.zip /home/ubuntu/aws

# SSH keys of core team
mkdir -p /home/ubuntu/.ssh

# ~/.ssh/authorized_keys and ~/.ssh/authorized_keys2 are cleaned out, so we'll put them near
aws lambda invoke --region us-east-1 --function-name team-keys-lambda /home/ubuntu/.ssh/core_team_authorized_keys
chown ubuntu: /home/ubuntu/.ssh -R
chmod 0700 /home/ubuntu/.ssh
