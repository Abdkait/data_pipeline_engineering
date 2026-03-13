#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Set Java environment variables for Spark
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

# Set Spark environment variables
export SPARK_HOME="$(python -c 'import pyspark; import os; print(os.path.dirname(pyspark.__file__))')"

echo "Environment activated!"
echo "Java version:"
java -version
echo ""
echo "To start Jupyter Notebook, run:"
echo "jupyter notebook"
