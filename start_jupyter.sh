#!/bin/bash

# Quick start script for Jupyter Notebook with Spark

cd "$(dirname "$0")"

echo "🚀 Запуск Jupyter Notebook для ДЗ..."
echo ""

source venv/bin/activate

export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"

echo "✅ Окружение активировано"
echo "✅ Java: $(java -version 2>&1 | head -n 1)"
echo "✅ PySpark: $(python -c 'import pyspark; print(pyspark.__version__)')"
echo ""
echo "📂 Открываем Jupyter Notebook..."
echo ""

jupyter notebook hw/hw_7/task.ipynb
