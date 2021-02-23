import sqlite3
import os
import csv
from config import output_file, output_headers


def startup_db():
    db_file = 'test.sqlite'
    try:
        os.remove(db_file)
    except FileNotFoundError:
        pass
    db = sqlite3.connect(db_file)
    db.execute('''
    create table taxi_tip (
        tipAmount real,
        Year int,
        neighborhood text,
        borough text
    )
    ''')
    return db


def startup():
    try:
        os.remove(output_file)
    except FileNotFoundError:
        pass

    with open(output_file, newline='', encoding='utf-8', mode='w') as f:
        writer = csv.writer(f)
        writer.writerow(output_headers)


def truncate(db):
    cursor = db.cursor()
    cursor.execute('''
    delete from taxi_tip
    ''')


def append_to_csv(db):
    """
    responsible for appending rows to the output csv file from the sqlite database
    and truncating the table
    """
    cursor = db.cursor()
    resp = cursor.execute('''
    select year
        ,borough
        ,neighborhood
        ,avg(tipAmount) as avg_tip
        ,case
            when ct % 2 = 1
                then avg(tipAmount) filter(where rn = ct / 2 + 1)
            else
                avg(tipAmount) filter(where rn in (ct / 2, ct / 2 + 1))
         end median_tip
    from (
        select year
            ,borough
            ,neighborhood
            ,tipAmount
            ,count(*) over (partition by year, borough, neighborhood) as ct
            ,rn
        from (
            select year
                ,borough
                ,neighborhood
                ,tipAmount
                ,row_number() over (partition by year, borough, neighborhood order by tipAmount desc) rn
            from taxi_tip
        ) g
    ) g
    group by year
        ,borough
        ,neighborhood
    ''')

    with open(output_file, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in resp.fetchall():
            writer.writerow(row)

    truncate(db)
