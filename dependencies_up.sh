#!/usr/bin/env bash

set -euo pipefail

wait-for() {
  local dependency=${1}
  local dependency_check=${2}

  echo "Waiting for $dependency to launch....."

  while [[ -z "$(eval "${dependency_check}")" ]];
  do
    echo "Waiting ($dependency)..."
    sleep 3
  done

  echo "$dependency is available"
}

docker compose -f docker-compose.yml up -d localstack

wait-for 'Localstack' 'nc -z 127.0.0.1 4566 2>&1 > /dev/null && echo success'