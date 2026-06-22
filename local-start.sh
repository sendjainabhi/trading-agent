#!/bin/bash

PORT=9091

echo "🔨 Building and starting Trading Agent locally on port $PORT ..."
mvn clean compile spring-boot:run -Dspring-boot.run.jvmArguments="-Dserver.port=$PORT"
