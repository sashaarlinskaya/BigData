#!/usr/bin/env python3
"""
Анализ данных продаж с использованием Hive через PyHive
Задача: найти топ-10 товаров по количеству продаж
"""
from pyhive import hive
import subprocess
import os
import sys
import time

def run_hive_query(host='localhost', port=10000, username='hadoop', database='default'):
    """Создать соединение с Hive и выполнить запросы"""
    try:
        # Создаем соединение с Hive
        connection = hive.Connection(
            host=host,
            port=port,
            username=username,
            database=database
        )
        cursor = connection.cursor()
        print("✓ Успешное подключение к Hive")
        return cursor, connection
    except Exception as e:
        print(f"✗ Ошибка подключения к Hive: {e}")
        sys.exit(1)

def check_hadoop_services():
    """Проверить запущены ли необходимые сервисы Hadoop"""
    print("\n=== Проверка сервисов Hadoop ===")
    
    services = [
        'hadoop',
        'hive',
        'hdfs'
    ]
    
    for service in services:
        try:
            result = subprocess.run(
                ['jps'], 
                capture_output=True, 
                text=True, 
                check=True
            )
            if service in result.stdout.lower():
                print(f"✓ Сервис {service} запущен")
            else:
                print(f"✗ Сервис {service} не запущен")
        except Exception as e:
            print(f"Ошибка при проверке сервиса {service}: {e}")

def upload_data_to_hdfs():
    """Загрузить данные в HDFS"""
    print("\n=== Загрузка данных в HDFS ===")
    
    local_file = '/opt/data/myfile.csv'
    hdfs_path = '/user/hadoop/sales_data/'
    
    # Проверяем существование локального файла
    if not os.path.exists(local_file):
        print(f"✗ Файл {local_file} не найден")
        sys.exit(1)
    
    print(f"✓ Локальный файл найден: {local_file}")
    
    try:
        # Создаем директорию в HDFS
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_path], check=True)
        print(f"✓ Создана директория HDFS: {hdfs_path}")
        
        # Копируем файл в HDFS
        subprocess.run(['hdfs', 'dfs', '-put', '-f', local_file, hdfs_path], check=True)
        print(f"✓ Файл загружен в HDFS: {hdfs_path}myfile.csv")
        
        # Проверяем загрузку
        result = subprocess.run(
            ['hdfs', 'dfs', '-ls', f'{hdfs_path}myfile.csv'], 
            capture_output=True, 
            text=True, 
            check=True
        )
        print(f"✓ Подтверждение загрузки в HDFS")
        
    except subprocess.CalledProcessError as e:
        print(f"✗ Ошибка при работе с HDFS: {e}")
        sys.exit(1)

def create_sales_table(cursor):
    """Создать таблицу продаж в Hive"""
    print("\n=== Создание таблицы в Hive ===")
    
    create_table_query = """
    CREATE EXTERNAL TABLE IF NOT EXISTS sales_data (
        InvoiceNo STRING,
        StockCode STRING,
        Description STRING,
        Quantity INT,
        InvoiceDate STRING,
        UnitPrice DOUBLE,
        CustomerID STRING,
        Country STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hadoop/sales_data/'
    TBLPROPERTIES ("skip.header.line.count"="1")
    """
    
    try:
        cursor.execute(create_table_query)
        print("✓ Таблица sales_data создана успешно")
    except Exception as e:
        print(f"✗ Ошибка при создании таблицы: {e}")

def verify_data_loaded(cursor):
    """Проверить загрузку данных"""
    print("\n=== Проверка загрузки данных ===")
    
    try:
        # Проверяем количество записей
        cursor.execute("SELECT COUNT(*) FROM sales_data")
        count_result = cursor.fetchone()
        print(f"✓ Загружено записей в таблицу: {count_result[0]:,}")
        
        # Показываем пример данных
        cursor.execute("SELECT * FROM sales_data LIMIT 5")
        sample_data = cursor.fetchall()
        print("\nПример данных (первые 5 строк):")
        print("InvoiceNo | StockCode | Description | Quantity | UnitPrice")
        print("-" * 60)
        for row in sample_data:
            print(f"{row[0]:<9} | {row[1]:<9} | {row[2][:20]:<20} | {row[3]:<8} | {row[5]:<8}")
            
    except Exception as e:
        print(f"✗ Ошибка при проверке данных: {e}")

def analyze_top_products(cursor):
    """Анализ топ-10 товаров по количеству продаж"""
    print("\n" + "="*80)
    print("АНАЛИЗ: ТОП-10 ТОВАРОВ ПО КОЛИЧЕСТВУ ПРОДАЖ")
    print("="*80)
    
    analysis_query = """
    SELECT 
        StockCode,
        Description,
        SUM(Quantity) AS TotalQuantity,
        COUNT(*) AS TransactionCount,
        ROUND(AVG(UnitPrice), 2) AS AvgUnitPrice,
        ROUND(SUM(Quantity * UnitPrice), 2) AS TotalRevenue
    FROM sales_data
    WHERE 
        NOT STARTSWITH(InvoiceNo, 'C')  -- Исключаем отмененные заказы
        AND Quantity > 0                 -- Исключаем возвраты
        AND Description IS NOT NULL      -- Исключаем товары без описания
        AND UnitPrice > 0                -- Исключаем товары с нулевой ценой
    GROUP BY StockCode, Description
    ORDER BY TotalQuantity DESC
    LIMIT 10
    """
    
    try:
        cursor.execute(analysis_query)
        results = cursor.fetchall()
        
        print("\n{:^10} | {:<40} | {:>12} | {:>8} | {:>10} | {:>12}".format(
            "Код", "Описание", "Кол-во", "Транзакции", "Ср. цена", "Выручка"
        ))
        print("-" * 110)
        
        for row in results:
            stock_code, description, total_qty, transactions, avg_price, revenue = row
            print("{:>10} | {:<40} | {:>12,} | {:>8,} | {:>10.2f} | {:>12,.2f}".format(
                stock_code, 
                description[:40], 
                total_qty, 
                transactions, 
                avg_price, 
                revenue
            ))
        
        return results
        
    except Exception as e:
        print(f"✗ Ошибка при выполнении анализа: {e}")
        return []

def create_results_table(cursor):
    """Создать таблицу для результатов"""
    print("\n=== Создание таблицы для результатов ===")
    
    create_results_table_query = """
    CREATE TABLE IF NOT EXISTS top_10_products (
        StockCode STRING,
        Description STRING,
        TotalQuantity INT,
        TransactionCount INT,
        AvgUnitPrice DOUBLE,
        TotalRevenue DOUBLE
    )
    STORED AS ORC
    """
    
    insert_results_query = """
    INSERT OVERWRITE TABLE top_10_products
    SELECT 
        StockCode,
        Description,
        SUM(Quantity) AS TotalQuantity,
        COUNT(*) AS TransactionCount,
        ROUND(AVG(UnitPrice), 2) AS AvgUnitPrice,
        ROUND(SUM(Quantity * UnitPrice), 2) AS TotalRevenue
    FROM sales_data
    WHERE 
        NOT STARTSWITH(InvoiceNo, 'C')
        AND Quantity > 0
        AND Description IS NOT NULL
        AND UnitPrice > 0
    GROUP BY StockCode, Description
    ORDER BY TotalQuantity DESC
    LIMIT 10
    """
    
    try:
        cursor.execute(create_results_table_query)
        print("✓ Таблица top_10_products создана")
        
        cursor.execute(insert_results_query)
        print("✓ Результаты сохранены в таблицу top_10_products")
        
    except Exception as e:
        print(f"✗ Ошибка при работе с таблицей результатов: {e}")

def export_results_to_hdfs(cursor):
    """Экспортировать результаты в HDFS"""
    print("\n=== Экспорт результатов в HDFS ===")
    
    export_query = """
    INSERT OVERWRITE DIRECTORY '/user/hadoop/output/top_products'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    SELECT * FROM top_10_products
    """
    
    try:
        cursor.execute(export_query)
        print("✓ Результаты экспортированы в HDFS: /user/hadoop/output/top_products")
        
        # Проверяем экспортированные файлы
        time.sleep(2)  # Даем время на запись
        result = subprocess.run(
            ['hdfs', 'dfs', '-ls', '/user/hadoop/output/top_products/'],
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            print("✓ Файлы в HDFS:")
            for line in result.stdout.split('\n'):
                if 'part-' in line:
                    print(f"  - {line.split()[-1]}")
        
        # Показываем содержимое результатов
        print("\nСодержимое результатов:")
        subprocess.run(['hdfs', 'dfs', '-cat', '/user/hadoop/output/top_products/000000_0 | head -5'], 
                      shell=True)
        
    except Exception as e:
        print(f"✗ Ошибка при экспорте в HDFS: {e}")

def cleanup(cursor):
    """Очистка таблиц (опционально)"""
    print("\n=== Очистка ===")
    
    try:
        cursor.execute("DROP TABLE IF EXISTS sales_data")
        cursor.execute("DROP TABLE IF EXISTS top_10_products")
        print("✓ Временные таблицы удалены")
        
        subprocess.run(['hdfs', 'dfs', '-rm', '-r', '/user/hadoop/sales_data/'], 
                      capture_output=True)
        subprocess.run(['hdfs', 'dfs', '-rm', '-r', '/user/hadoop/output/top_products/'], 
                      capture_output=True)
        print("✓ Временные данные в HDFS удалены")
        
    except Exception as e:
        print(f"⚠ Ошибка при очистке: {e}")

def main():
    """Основная функция"""
    print("=== АНАЛИЗ ПРОДАЖ С ИСПОЛЬЗОВАНИЕМ HIVE ===")
    print(f"Время начала: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # 1. Проверка сервисов
        check_hadoop_services()
        
        # 2. Загрузка данных в HDFS
        upload_data_to_hdfs()
        
        # 3. Подключение к Hive
        cursor, connection = run_hive_query()
        
        # 4. Создание таблицы
        create_sales_table(cursor)
        
        # 5. Проверка данных
        verify_data_loaded(cursor)
        
        # 6. Анализ топ-10 товаров
        results = analyze_top_products(cursor)
        
        if results:
            # 7. Сохранение результатов в таблицу
            create_results_table(cursor)
            
            # 8. Экспорт в HDFS
            export_results_to_hdfs(cursor)
            
            print(f"\n🎉 Анализ завершен успешно!")
            print(f"Найдено топ-10 товаров по количеству продаж")
            print(f"Результаты сохранены в HDFS: /user/hadoop/output/top_products")
        
        # 9. Закрытие соединения
        connection.close()
        
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()