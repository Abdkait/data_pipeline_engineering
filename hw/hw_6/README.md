# Data Pipeline Engineering - HSE Course

## Домашнее задание 6: Оптимизация Spark DataFrame

### Установка окружения

Все необходимые компоненты уже установлены:
- ✅ Python 3.14.2
- ✅ Java 17 (OpenJDK)
- ✅ Jupyter Notebook
- ✅ PySpark 4.1.1
- ✅ Pandas, Matplotlib, Seaborn

### Проверка установки

Запустите тестовый скрипт:
```bash
source venv/bin/activate
python test_setup.py
```

### Как запустить Jupyter Notebook локально

1. **Активируйте окружение:**
   ```bash
   cd /Users/abdlait/Desktop/VSCode/study/HSE/data_pipeline_engineering
   source setup_env.sh
   ```

2. **Запустите Jupyter Notebook:**
   ```bash
   jupyter notebook
   ```

3. **Откройте файл:**
   - В браузере откроется интерфейс Jupyter
   - Перейдите в папку `hw/`
   - Откройте `hw_6.ipynb`

4. **Запустите все ячейки:**
   - Нажмите `Kernel` → `Restart & Run All`
   - Или запускайте ячейки по одной через `Shift + Enter`

### Альтернативный способ (через VS Code)

Если вы работаете в VS Code/Cursor:
1. Откройте файл `hw_6.ipynb`
2. VS Code автоматически предложит установить расширение Jupyter
3. Выберите интерпретатор Python из виртуального окружения (`venv/bin/python`)
4. Запускайте ячейки прямо в редакторе

### Структура проекта

```
data_pipeline_engineering/
├── hw/
│   ├── hw_6.ipynb              # Домашнее задание
│   ├── OnlineRetail.csv        # Датасет (76MB, 1M строк)
│   └── generate_dataset.py     # Скрипт генерации данных
├── lec/                        # Лекции (PDF)
├── venv/                       # Виртуальное окружение Python
├── requirements.txt            # Зависимости Python
├── setup_env.sh               # Скрипт активации окружения
└── README.md                  # Этот файл
```

### Содержание домашнего задания

**Основные кейсы (DataFrame vs RDD):**
1. Множественные агрегации (SUM, AVG, MIN, MAX, COUNT)
2. Оконные функции (Top-N товаров по странам)
3. Условная логика (классификация заказов)

**Дополнительное задание (SQL vs DataFrame API):**
1. Агрегации с HAVING
2. JOIN с подзапросами

Каждый кейс включает:
- Реализацию на DataFrame и RDD/SQL
- Измерение производительности
- Планы выполнения (explain())
- Объяснение оптимизаций Catalyst/Tungsten

### Датасет

**Online Retail Dataset** - ~1 млн транзакций розничных продаж

Колонки:
- `InvoiceNo` - номер заказа
- `StockCode` - код товара
- `Description` - описание товара
- `Quantity` - количество
- `InvoiceDate` - дата заказа
- `UnitPrice` - цена за единицу
- `CustomerID` - ID покупателя
- `Country` - страна
- `Revenue` - выручка (вычисляемая колонка)

### Полезные команды

**Проверить установку:**
```bash
source venv/bin/activate
python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"
java -version
```

**Переустановить зависимости:**
```bash
source venv/bin/activate
pip install -r requirements.txt
```

**Сгенерировать новый датасет:**
```bash
cd hw/
source ../venv/bin/activate
python generate_dataset.py
```

### Troubleshooting

**Проблема:** Java не найдена
```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"
```

**Проблема:** Jupyter не запускается
```bash
source venv/bin/activate
pip install --upgrade jupyter notebook
```

**Проблема:** Spark падает с OutOfMemory
- Уменьшите размер датасета в `generate_dataset.py` (например, до 500,000 строк)
- Или увеличьте память в конфигурации Spark в ноутбуке

### Ресурсы

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Catalyst Optimizer](https://databricks.com/glossary/catalyst-optimizer)
