## Гарвилюк Назар, КМ-82
## Варіант 10 
## Порівняти середній бал з Української мови та літератури у кожному регіоні
## у 2020 та 2019 роках серед тих кому було зараховано тест

import csv
import itertools
import psycopg2
import psycopg2.errorcodes
import time
import datetime

# configuration settings
from config import datasets_folder

def apply_query(query_, conn, cursor):
    cursor.execute(query_)
    conn.commit()

def fill_csv_into_table(fname, header, year, res_file, conn, cursor):
    """Заповнює таблицю з csv-файлу. Втрата з'єднання з базою даних передбачена. Створює лог, в якому записує, час
    виконання запиту.
    fname -- назва csv-файлу з даними.
    header -- назви колонок
    year -- рік, якому відповідає даний csv-файл.
    res_file -- файл для запису результату.
    conn -- об'єкт з'єднання з БД.
    cursor -- курсор БД.
    """

    time_1 = datetime.datetime.now() 
    res_file.write(" START ---- " + str(time_1) + " ---- " + fname + '\n')
    
    with open(fname, "r", encoding="cp1251") as csv_file:
        print(f" Reading CSV ---- {csv_file}")
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        inserted = 0
        batch_size = 200
        
        finished = False

        while not finished: 
            try:
                insert_query = '''INSERT INTO table_zno (year, ''' + ', '.join(header) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1
                    # обробляємо запис: оточуємо всі текстові рядки одинарними лапками, замінюємо в числах кому на крапку
                    for key in row:
                        if row[key] == 'null':
                            pass
                        elif key.lower() != 'birth' and 'ball' not in key.lower():
                            row[key] = "'" + row[key].replace("'", "''") + "'"
                        elif 'ball100' in key.lower():
                            row[key] = row[key].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'

                    # якщо набралося 100 рядків -- коммітимо транзакцію
                    if count == batch_size:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cursor.execute(insert_query)
                        conn.commit()
                        inserted += 1
                        insert_query = '''INSERT INTO table_zno (year, ''' + ', '.join(header) + ') VALUES '
                    
                # якщо досягли кінця файлу -- коммітимо транзакцію
                if count != 0:
                    insert_query = insert_query.rstrip(',') + ';'
                    cursor.execute(insert_query)
                    conn.commit()
                finished = True

            except psycopg2.OperationalError as e:
                # якщо з'єднання з базою даних втрачено
                if e.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print(" DATABASE IS SHUT DOWN ---- ")
                    res_file.write(" EXCEPTION ----" + str(datetime.datetime.now()) + " - connection lost\n")
                    restored = False
                    while not restored:
                        try:
                            conn = psycopg2.connect(dbname='postgres', user='postgres', 
                                password='postgres', host='localhost', port="5432")
                            cursor = conn.cursor()
                            res_file.write(" RESTORING CONNECTION ---- " + str(datetime.datetime.now()) + " - restored\n")
                            restored = True
                        except psycopg2.OperationalError as e:
                            pass

                    csv_file.seek(0,0)
                    csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'), 
                        inserted * batch_size, None)

    end_time = datetime.datetime.now()
    res_file.write(" FINISHED ---- " + str(end_time) + " -- totally handled\n")
    res_file.write(' TOTAL TIME ---- ' + str(end_time - time_1) + '\n')

    return conn, cursor

def table_start(conn, cursor, table_name='table_zno'):
    """ вертає заголовок csv-файлу після створення таблиці. 
    f -- назва csv-файлу."""
    with open(f"{datasets_folder}/Odata2019File.csv", "r", encoding="cp1251") as csv_file:
        col_names = csv_file.readline().split(';')
        col_names = [v.strip('"').rstrip('"\n') for v in col_names]
        # формуємо запит на створення таблиці
        create_sql = f'''CREATE TABLE IF NOT EXISTS {table_name} ('''
        columns_info = "\n\tYear INT,"
        for name in col_names:
            if name == 'Birth':
                columns_info += '\n\t' + name + ' INT,'
            elif 'Ball' in name:
                columns_info += '\n\t' + name + ' REAL,'
            elif name == "OUTID":
                columns_info += '\n\t' + name + ' VARCHAR(40) PRIMARY KEY,'
            else:
                columns_info += '\n\t' + name + ' VARCHAR(255),'
        create_sql += columns_info.rstrip(',') + '\n);'
        
        # створення таблиці
        apply_query(create_sql, conn, cursor)
        return col_names


def statistical_query(cursor, result_file):
    """Виконує статистичний запит до таблиці та записує результат у новий csv-файл. """
    select_query = '''
    SELECT year, regname, avg(ukrBall100)
    FROM table_zno
    WHERE ukrteststatus = 'Зараховано'
    GROUP BY regname, year;
    '''
    cursor.execute(select_query)

    with open(f'{result_file}.csv', 'w', encoding="utf-8") as new_csv:
        csv_w = csv.writer(new_csv)
        csv_w.writerow([ 'Рік', 'Область', 'Середній. бал з української'])
        for row in cursor:
            csv_w.writerow(row)

if __name__ == "__main__":
    conn = psycopg2.connect(dbname='postgres', user='postgres', 
                            password='postgres', host='localhost', port="5432")
    cursor = conn.cursor()
    TABLE_NAME = 'table_zno'
    cursor.execute(f'DROP TABLE IF EXISTS {TABLE_NAME};') # очищуємо усе перед початком
    conn.commit()
    header_ = table_start(conn, cursor, TABLE_NAME)
    logs_file = open('summary.log', 'w+', encoding='cp1251')       
    conn, cursor = fill_csv_into_table(f"{datasets_folder}/Odata2019File.csv", header_, 2019, logs_file, conn, cursor)
    conn, cursor = fill_csv_into_table(f"{datasets_folder}/Odata2020File.csv", header_, 2020, logs_file, conn, cursor)    
    logs_file.close()
    statistical_query(cursor, 'result')
    #conn.commit()
    cursor.close()
    conn.close()