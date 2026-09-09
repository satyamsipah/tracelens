#!/usr/bin/env bash
# Renders deploy/k8s/manifests.yaml from the Helm chart, which is the single
# source of truth. Committing the rendered output means a plain
# `kubectl apply -f` path exists for people who do not want Helm, without
# two copies of the same YAML drifting apart.
#
# Usage: make k8s-manifests
set -euo pipefail

cd "$(dirname "$0")/.."

helm template tracelens deploy/helm/tracelens \
  --namespace tracelens \
  --set secrets.clickhousePassword=REPLACE_ME \
  --set web.publicAPIBase=https://api.tracelens.example.com \
  > /tmp/tracelens-render.yaml

{
  echo "# GENERATED FILE -- DO NOT EDIT."
  echo "#"
  echo "# Rendered from deploy/helm/tracelens by scripts/render-k8s.sh"
  echo "# (make k8s-manifests). Edit the chart, not this file."
  echo "#"
  echo "# Before applying:"
  echo "#   1. Replace REPLACE_ME in the Secret, or delete it and create the"
  echo "#      Secret out of band (preferred -- see deploy/k8s/README.md)."
  echo "#   2. Set the real public API URL for the web deployment."
  echo "#   3. Point clickhouse/kafka addresses at your actual clusters."
  echo "#"
  echo "# kubectl create namespace tracelens"
  echo "# kubectl -n tracelens apply -f deploy/k8s/manifests.yaml"
  cat /tmp/tracelens-render.yaml
} > deploy/k8s/manifests.yaml

echo "wrote deploy/k8s/manifests.yaml ($(grep -c '^kind:' deploy/k8s/manifests.yaml) resources)"
