# ITOG
Итоговая работа


Создаем контейнер PostgreSQL

Запускаем контейнер
sudo docker-compose up

Проверяем наличие файлов
sudo docker exec -it spark-db-1 bash
cd data
ls -l
Если Docker не нашел файл, то на его месте он создает каталог
drwxr-xr-x 2 root root 4096 Aug 30 17:19 Clients.csv
Это ошибка в yml

Подключаемся к psql
sudo docker exec -it spark-db-1 psql -U postgres

Создаем базу данных и подключаемся к ней
CREATE DATABASE db;
\l
\c db

Создаем таблицы
CREATE TABLE account (AccountID integer PRIMARY KEY, AccountNum double precision, ClientId integer, DateOpen date);

CREATE TABLE clients (ClientId integer PRIMARY KEY, ClientName character varying (30), Type character varying (2), Form character varying (30), RegisterDate date);

CREATE TABLE operation (AccountDB integer, AccountCR integer, DateOp date, Amount character varying (30), Currency character varying (10), Comment text);

CREATE TABLE rate (Currency character varying (10), Rate character varying (30), RateDate date, PRIMARY KEY (Currency, RateDate));



Копируем в таблицы данные из CSV
\COPY account FROM 'data/Account.csv' DELIMITER ';' CSV HEADER;

\COPY clients FROM 'data/Clients.csv' DELIMITER ';' CSV HEADER;

\COPY operation FROM 'data/Operation.csv' DELIMITER ';' CSV HEADER;

\COPY rate FROM 'data/Rate.csv' DELIMITER ';' CSV HEADER;


В папку spark/jars копируем файл postgresql-42.6.0.jar для возможности работать с PostgreSQL по JDBC
sudo docker cp postgresql-42.6.0.jar spark-spark-master-1:/opt/bitnami/spark/jars/postgresql-42.6.0.jar

Копируем файл с приложением Spark
sudo docker cp primer1.py spark-spark-master-1:/opt/bitnami/spark/primer1.py

Заходим в spark-master 
sudo docker exec -it spark-spark-master-1 bash

Запускаем приложение, выполнив
./bin/spark-submit primer1.py




