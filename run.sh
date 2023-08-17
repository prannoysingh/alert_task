#!/bin/bash

set -o errexit

docker compose down
docker compose up --build
