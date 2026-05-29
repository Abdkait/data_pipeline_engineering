#!/usr/bin/env bash
#
# Runs the ConsultantPlus Session Analytics Spark job locally (no spark-submit required).
# The fat jar bundles Spark, so we launch it with `java -jar`. On Java 17 Spark needs a set
# of --add-opens flags, which are provided below.
#
# Usage: ./run.sh [INPUT_DIR] [OUTPUT_DIR]
# Defaults: INPUT_DIR=Data, OUTPUT_DIR=output
set -euo pipefail

INPUT_DIR="${1:-Data}"
OUTPUT_DIR="${2:-output}"
JAR="target/session-analytics-1.0.0.jar"

# Use Java 17 if available (Spark 3.5 does not support newer JDKs).
if [ -x "/opt/homebrew/opt/openjdk@17/bin/java" ]; then
  JAVA_BIN="/opt/homebrew/opt/openjdk@17/bin/java"
else
  JAVA_BIN="java"
fi

ADD_OPENS=(
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
)

rm -rf "$OUTPUT_DIR"
"$JAVA_BIN" "${ADD_OPENS[@]}" \
  -Dspark.driver.bindAddress=127.0.0.1 \
  -Dspark.driver.host=127.0.0.1 \
  -jar "$JAR" "$INPUT_DIR" "$OUTPUT_DIR"
