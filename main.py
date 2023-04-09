import datetime
import time

import xml.etree.ElementTree as ET
from urllib.request import urlopen

import apiclient
import httplib2
import psycopg2
from oauth2client.service_account import ServiceAccountCredentials

check_period = 300  # пауза 5 минут перед новым запросом

con = psycopg2.connect(
                dbname='postgres',
                user='postgres',
                host='localhost',
                port='5432',
                password='9934'
                )


cur = con.cursor()
CREDENTIALS_FILE = 'myproject-383010-6cd3e8cd4885.json'  # Имя файла с закрытым ключом, вы должны подставить свое

credentials = ServiceAccountCredentials.from_json_keyfile_name(
                                            CREDENTIALS_FILE,
                                            ['https://www.googleapis.com/auth/spreadsheets',
                                                'https://www.googleapis.com/auth/drive']
                                            )

httpAuth = credentials.authorize(httplib2.Http())  # Авторизуемся в системе
service = apiclient.discovery.build(
                                'sheets',
                                'v4',
                                http=httpAuth,
                                )  # Выбираем работу с таблицами и 4 версию API
spreadsheetId = "1E_FXu1yrS_af-yRI83_cuUuTNivSn4w8EvlS8Vazqdg"  # сохраняем идентификатор файла
driveService = apiclient.discovery.build(
                                    'drive',
                                    'v3',
                                    http=httpAuth,
                                    )  # Выбираем работу с Google Drive и 3 версию API
access = driveService.permissions().create(
    fileId=spreadsheetId,
    body={'type': 'user', 'role': 'writer', 'emailAddress': 'mailohotdog@gmail.com'},  # Открываем доступ на редактирование
    fields='id'
).execute()
ranges = ["Лист1!A1:D1000000"]  # Указываем рендж на листе с которого хотим взять инфу
results = service.spreadsheets().values().batchGet(
                                    spreadsheetId=spreadsheetId,
                                    ranges=ranges,
                                    valueRenderOption='FORMATTED_VALUE',
                                    dateTimeRenderOption='FORMATTED_STRING',
                                    ).execute()  # Получаем данные из Google sheet
sheet_values = results['valueRanges'][0]['values']  # берем нужные данные

cur.execute('''
CREATE TABLE IF NOT EXISTS test(
    № INTEGER,
    заказ_№ serial PRIMARY KEY,
    стоимость_$ INTEGER,
    срок_поставки DATE,
    стоимость_в_руб INTEGER
);
''')  # Создаем таблицу в базе


def exchange_rate():
    """Получаем курс валюты с сайта ЦБ РФ"""
    with urlopen("https://www.cbr.ru/scripts/XML_daily.asp", timeout=10) as r:
        return ET.parse(r).findtext('.//Valute[@ID="R01235"]/Value')


def insert_value():
    """Напонляем таблицу данными"""
    cur.execute('DELETE FROM test;')
    sheet_values.pop(0)
    for value in sheet_values:
        rus_cost = int(value[2])*float(exchange_rate().replace(",", "."))
        value[3] = datetime.datetime.strptime(
                                        value[3].replace(" ", ""),
                                        "%d.%m.%Y"
                                        ).date()
        value.append(f"{rus_cost:.0f}")  # Добавляем стоимость в рублях в данные
        value = tuple(value)
        cur.execute(
            """INSERT INTO test (
                            №,
                            заказ_№,
                            стоимость_$,
                            срок_поставки,
                            стоимость_в_руб
                            ) VALUES(%s, %s, %s, %s, %s);""",
            value
        )  # наполняем таблицу данными
    con.commit()
    con.close()


def main():
    while True:
        insert_value()
        time.sleep(check_period)


main()
