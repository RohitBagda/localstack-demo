# localstack-demo

A simple Kotlin application that demonstrates how Localstack can be used to mock AWS services locally for system testing.

## Prerequisites

* Docker & Docker Compose (comes bundled with [Docker Desktop](https://www.docker.com/products/docker-desktop/))
* Java 11 or higher

# Usage

## Start dependencies
`./dependencies_up.sh`

## Run application
`./gradlew build`

`./gradlew run`

## Run tests
`./gradlew test`

# Stop dependencies
`./dependencies_down.sh`
