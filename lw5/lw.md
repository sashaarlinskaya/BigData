# Лабораторная работа 5.1. Развертывание и настройка Hadoop Анализ данных с использованием экосистемы Hadoop
## Титульный лист

**Дисциплина:** Инструменты для хранения и обработки больших данных  
**Тема:** Сравнение подходов хранения больших данных  
**Вариант:** 3  

**Выполнила:** Арлинская Александра Викторовна  
**Проверил:** Босенко Тимур Муртазович  
**Курс обучения:** 4  
**Форма обучения:** очная  

**Институт цифрового образования**  
**Департамент информатики, управления и технологий**  
**Московский городской педагогический университет**  
**Москва 2025**

## Цель работы

получить практические навыки развертывания одноузлового кластера Hadoop, освоить базовые операции с распределенной файловой системой HDFS, выполнить загрузку и простейшую обработку данных, а также научиться
выгружать результаты для последующего анализа и визуализации во внешней среде (Jupyter Notebook / Google Colab).



**Аналитическая задача.** Найти топ-10 товаров по количеству продаж (вариант 3).

**Источник данных.** [[https://www.kaggle.com/datasets/usgs/earthquake-database]

---

## 🏗 Архитектура системы

```
┌──────────────────────────────────────────────────────────┐
│              Docker Container (Ubuntu 20.04)             │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐    │
│  │  NameNode    │    │   DataNode   │    │Resource  │    │
│  │  (HDFS)      │◄──►│   (HDFS)     │    │ Manager  │    │
│  └──────────────┘    └──────────────┘    └──────────┘    │
│         │                                             │  │
│         │                                             │  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────┐    │
│  │ Secondary    │    │   DataNode   │    │   Node   │    │
│  │   NameNode   │    │   (HDFS)     │    │ Manager  │    │
│  └──────────────┘    └──────────────┘    └──────────┘    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │           HDFS File System                       │    │
│  │    /user/hadoop/input/database.csv               │    │
│  └──────────────────────────────────────────────────┘    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │         Python / PySpark Analysis                │    │
│  │    • Pandas для быстрого анализа                 │    │
│  │    • PySpark для больших данных                  │    │
│  │    • Jupyter для визуализации                    │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                │
                │ HTTP
                ▼
        http://localhost:9870 (HDFS UI)
        http://localhost:8088 (YARN UI)
```


## 🚀 Запуск проекта

### Шаг 1. Подготовили рабочее место 

```bash

# Убедились, что файлы на месте
ls -la hadoop/
ls -la scripts/
ls -la notebooks/
```

**Проверили:** Увидели 6 файлов в директории `hadoop/`:
- `workers`
- `core-site.xml`
- `hdfs-site.xml`
- `yarn-site.xml`
- `mapred-site.xml`
- `log4j.properties`

### Шаг 2. Запустили Docker контейнер

```bash
# Запустили контейнер
# docker compose up --build # if new data rewrite database.csv
docker compose up -d

# Просмотрели логи (подождите 60-90 секунд)
docker compose logs -f hadoop



### Шаг 3. Подключились к контейнеру

```bash
# Открыть терминал внутри контейнера
docker compose exec hadoop bash

# Проверили, что вы внутри контейнера
hostname  # должен показать: hadoop
```

### Шаг 4. Проверили компоненты Hadoop

```bash
# Проверить статус всех процессов
jps

# Должны увидеть:
# - NameNode
# - DataNode
# - SecondaryNameNode
# - ResourceManager
# - NodeManager
# - Jps
```


---

## 📁 Работа с HDFS

### Шаг 1. Созданали директории

```bash
# Создать директории для входных и выходных данных
hdfs dfs -mkdir -p /user/hadoop/input
hdfs dfs -mkdir -p /user/hadoop/output

# Проверить созданные директории
hdfs dfs -ls /user/hadoop/
```
<img width="912" height="267" alt="image" src="https://github.com/user-attachments/assets/c61e26e9-46b0-4e4f-92f8-7c5d163796ce" />


### Шаг 2. Загрузили данные

```bash
# Загрузить dataset в HDFS
hdfs dfs -put /opt/data/myfile.csv /user/hadoop/input/myfile.csv

# Проверить загрузку
hdfs dfs -ls -h /user/hadoop/input/

# Просмотреть размер файла
hdfs dfs -du -h /user/hadoop/input/
```



### Шаг 3. Просмотрели данные в HDFS

```bash
# Просмотрели первые строки файла из HDFS
hdfs dfs -cat /user/hadoop/input/myfile.csv | head -20

```
<img width="946" height="683" alt="image" src="https://github.com/user-attachments/assets/91549f3b-0df6-4886-8445-51215e8fa742" />


### Шаг 4. Веб-интерфейсы (открыли в браузере)

<img width="1848" height="569" alt="image" src="https://github.com/user-attachments/assets/1a9cd925-ea8e-42df-bf7e-8bfb4bd9421c" />
<img width="1905" height="1066" alt="image" src="https://github.com/user-attachments/assets/da145e33-d9bc-470a-b842-ccef943eba5c" />

---

## 🔍 Анализ данных

### Вариант 1. Pandas (быстрый анализ)

```bash
cd /opt/scripts

# Запустить анализ
python3 analyze_pandas.py

```
**Код analyze_pandas.py**
<img width="910" height="784" alt="image" src="https://github.com/user-attachments/assets/722e126e-9cb8-40c3-9005-ee5f1b66a110" />

**Результат analyze_pandas.py**
<img width="867" height="651" alt="image" src="https://github.com/user-attachments/assets/919a21e3-814e-4e0d-ac7e-4e6489146c5a" />


### Вариант 2. PySpark (для больших данных)

```bash
cd /opt/scripts

# Запустили анализ через Spark
python3 analyze_spark.py

```
**Код srark.py**

<img width="1485" height="802" alt="image" src="https://github.com/user-attachments/assets/d19a6865-13ec-43bf-9587-eae5d7082309" />

**Результат spark.py**
<img width="1019" height="713" alt="image" src="https://github.com/user-attachments/assets/293ab3bc-51f5-48c8-bf2f-adde9865b5af" />

### Вариант 3. Jupyter Notebook (визуализация)
### Шаг 1. Загрузка и очистка данных

<img width="1149" height="681" alt="image" src="https://github.com/user-attachments/assets/ef6184b3-f523-4fcd-8eaa-bd74921535b5" />

<img width="675" height="608" alt="image" src="https://github.com/user-attachments/assets/f7be7f21-553c-4510-9629-d8b7891ea2d2" />


### Шаг 2. Анализ топ-10 по количеству продаж
<img width="667" height="402" alt="image" src="https://github.com/user-attachments/assets/3192d68b-98fa-4afe-939e-6b32f6cc844a" />

### Шаг 3. Визуализация
<img width="1248" height="765" alt="image" src="https://github.com/user-attachments/assets/adcaab25-f37b-4fba-ab28-c9b6e0ec8df9" />

Цвет показывает ценовую категорию 
Зеленый - Высокая
Бирюзовый - Средняя
Синий - Низкая
