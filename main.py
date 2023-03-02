#!/usr/bin/python3

import os
import shutil
from datetime import datetime
import pandas as pd
import psycopg2

conn_src = psycopg2.connect(database = "bank",
                         host =     "de-edu-db.chronosavant.ru",
                         user =     "bank_etl",
                         password = "bank_etl_password",
                         port =     "5432")
conn_tgt = psycopg2.connect(database = "edu",
                          host =     "de-edu-db.chronosavant.ru",
                         user =     "de11an",
                         password = "peregrintook",
                         port =     "5432")
# Отключение автокоммита
conn_src.autocommit = False
conn_tgt.autocommit = False

# Создание курсора
cursor_src = conn_src.cursor()
cursor_tgt = conn_tgt.cursor()

# 1. Очистка стейджинговых таблиц #########################################################################################

cursor_tgt.execute("""
    DELETE FROM de11an.yasa_stg_cards;
    DELETE FROM de11an.yasa_stg_cards_del;
""")
cursor_tgt.execute("""
    DELETE FROM de11an.yasa_stg_accounts;
    DELETE FROM de11an.yasa_stg_accounts_del;
""")
cursor_tgt.execute("""
    DELETE FROM de11an.yasa_stg_clients;
    DELETE FROM de11an.yasa_stg_clients_del;
""")
cursor_tgt.execute("""
    DELETE FROM de11an.yasa_stg_transactions;
""")
cursor_tgt.execute("""
    DELETE FROM de11an.yasa_stg_blacklist;
    DELETE FROM de11an.yasa_dwh_fact_passport_blacklist;
""")
cursor_tgt.execute("""
    DELETE FROM de11an.yasa_stg_terminals;
    DELETE FROM de11an.yasa_stg_terminals_del;
""")

# 2. Захват данных из источника (измененных с момента последней загрузки) в стейджинг #################################################################################

# Для yasa_stg_cards
cursor_src.execute("SELECT * FROM info.cards")
data_list = cursor_src.fetchall()
cursor_tgt.executemany("""INSERT INTO de11an.yasa_stg_cards (card_num, account_num, create_dt, update_dt)
VALUES (%s,%s,%s,%s)""", data_list)

# Для yasa_stg_accounts
cursor_src.execute("SELECT * FROM info.accounts")
data_list = cursor_src.fetchall()
cursor_tgt.executemany("""INSERT INTO de11an.yasa_stg_accounts (account_num, valid_to, client, create_dt, update_dt)
VALUES (%s,%s,%s,%s,%s)""", data_list)

# Для yasa_stg_clients
cursor_src.execute("SELECT * FROM info.clients")
data_list = cursor_src.fetchall()
cursor_tgt.executemany("""INSERT INTO de11an.yasa_stg_clients (
        client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        create_dt,
        update_dt)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", data_list)

# Для yasa_stg_transactions
TRANSACTION_FILES_PREFIX = '/home/de11an/yasa/project/source/'
ARCHIVE_FILES_PREFIX = '/home/de11an/yasa/project/archive'

cursor_tgt.execute("""
    SELECT
        max_update_dt
    FROM de11an.yasa_meta
    WHERE schema_name='de11an' and source_name='transactions'
""")

last_transactions_date = cursor_tgt.fetchall()[0]
last_transactions_date = str(last_transactions_date).split('(')
last_transactions_date = (last_transactions_date[2]).split(')')
last_transactions_date = last_transactions_date[0]
last_transactions_date = datetime.strptime(last_transactions_date, '%Y, %m, %d')
# ищем файл для обработки
transactions_file = None
transactions_file_dt = None
for f in sorted(os.listdir(TRANSACTION_FILES_PREFIX)):
        if not f.startswith('transactions_'):
            continue
        _, file_date = f.split('_')
        file_date = file_date.split('.')
        file_date = file_date[0]
        transactions_file_dt = datetime.strptime(file_date, '%d%m%Y')
        if transactions_file_dt > last_transactions_date:
            transactions_file = f
            break

df = pd.read_csv(f'{TRANSACTION_FILES_PREFIX}/{transactions_file}', sep=';')
df['amount'] = df.amount.apply(lambda x: x.replace(',','.'))
df = df[['transaction_id', 'transaction_date', 'amount', 'card_num', 'oper_type', 'oper_result', 'terminal']]

cursor_tgt.executemany("""INSERT INTO de11an.yasa_stg_transactions(
    trans_id,
    trans_date,
    amt,
    card_num,
    oper_type,
    oper_result,
    terminal
)VALUES(%s,%s,%s,%s,%s,%s,%s)""", df.values.tolist())

# Для yasa_stg_blacklist
BLACKLIST_FILES_PREFIX = '/home/de11an/yasa/project/source/'
ARCHIVE_FILES_PREFIX = '/home/de11an/yasa/project/archive'

cursor_tgt.execute("""
    SELECT
        max_update_dt
    FROM de11an.yasa_meta
    WHERE schema_name='de11an' and source_name='blacklist';
""")

last_blacklist_date = cursor_tgt.fetchall()[0]
last_blacklist_date = str(last_blacklist_date).split('(')
last_blacklist_date = (last_blacklist_date[2]).split(')')
last_blacklist_date = last_blacklist_date[0]
last_blacklist_date = datetime.strptime(last_blacklist_date, '%Y, %m, %d')

# ищем файл для обработки
blacklist_file = None
blacklist_file_dt = None
for f in sorted(os.listdir(BLACKLIST_FILES_PREFIX)):
        if not f.startswith('passport_blacklist_'):
            continue
        _, file_date = f.split('st_')
        file_date = file_date.split('.')
        file_date = file_date[0]
        blacklist_file_dt = datetime.strptime(file_date, '%d%m%Y')
        if blacklist_file_dt > last_blacklist_date:
            blacklist_file = f
            break

# Формирование DataFrame
df = pd.read_excel(f'{BLACKLIST_FILES_PREFIX}/{blacklist_file}', index_col = False, header = 0)

cursor_tgt.executemany("""INSERT INTO de11an.yasa_stg_blacklist(
        enty_dt,
        passport_num
    ) VALUES (%s,%s)""", df.values.tolist())

# Для yasa_stg_terminals
TERMINAL_FILES_PREFIX = '/home/de11an/yasa/project/source/'
ARCHIVE_FILES_PREFIX = '/home/de11an/yasa/project/archive'

cursor_tgt.execute("""
    SELECT
        max_update_dt
    FROM de11an.yasa_meta
    WHERE schema_name='de11an' and source_name='terminals';
""")

last_terminals_date1 = cursor_tgt.fetchall()[0]

last_terminals_date = str(last_terminals_date1).split('(')
last_terminals_date = (last_terminals_date[2]).split(')')
last_terminals_date = last_terminals_date[0]
last_terminals_date = datetime.strptime(last_terminals_date, '%Y, %m, %d')

# ищем файл для обработки
terminals_file = None
terminals_file_dt = None
for f in sorted(os.listdir(TERMINAL_FILES_PREFIX)):
        if not f.startswith('terminals_'):
            continue
        _, file_date = f.split('_')
        file_date = file_date.split('.')
        file_date = file_date[0]
        terminals_file_dt = datetime.strptime(file_date, '%d%m%Y')
        if terminals_file_dt > last_terminals_date:
            terminals_file = f
            break

# Формирование DataFrame
df = pd.read_excel(f'{TERMINAL_FILES_PREFIX}/{terminals_file}', index_col = False, header = 0)
df['update_dt'] = terminals_file_dt.strftime('%Y-%m-%d')
df = df[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'update_dt']]

cursor_tgt.executemany(""" INSERT INTO de11an.yasa_stg_terminals(
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    update_dt
) VALUES (%s,%s,%s,%s,%s)""", df.values.tolist())

# 3. Захват в стейджинг ключей из источника полным срезом для вычисления удалений.###########################################

# Для yasa_stg_cards
cursor_tgt.execute("""INSERT INTO de11an.yasa_stg_cards_del (card_num)
    SELECT card_num FROM de11an.yasa_stg_cards;
""")

# Для yasa_stg_accounts
cursor_tgt.execute("""INSERT INTO de11an.yasa_stg_accounts_del (account_num)
    SELECT account_num FROM de11an.yasa_stg_accounts;
""")

# Для yasa_stg_clients
cursor_tgt.execute("""INSERT INTO de11an.yasa_stg_clients_del (client_id)
    SELECT client_id FROM de11an.yasa_stg_clients;
""")

# Для yasa_stg_terminals
cursor_tgt.execute(""" INSERT INTO de11an.yasa_stg_terminals_del(
    terminal_id)
    SELECT terminal_id
    FROM de11an.yasa_stg_terminals;
""")

# 4. Загрузка в приемник "вставок" на источнике.#############################################################################

# Для yasa_dwh_dim_cards_hist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_cards_hist (card_num, account_num, start_dt, end_dt, deleted_flg)
    SELECT
        stg.card_num,
        stg.account_num,
        stg.create_dt,
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'N'
    FROM de11an.yasa_stg_cards stg
    LEFT JOIN de11an.yasa_dwh_dim_cards_hist dim
        ON stg.card_num = dim.card_num
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
    WHERE dim.card_num is null;
""")

# Для yasa_dwh_dim_accounts_hist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_accounts_hist (account_num, valid_to, client, start_dt, end_dt, deleted_flg)
    SELECT
        stg.account_num,
        stg.valid_to,
        stg.client,
        stg.create_dt,
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'N'
    FROM de11an.yasa_stg_accounts stg
    LEFT JOIN de11an.yasa_dwh_dim_accounts_hist dim
        ON stg.account_num = dim.account_num
            and dim.deleted_flg = 'N'
    WHERE dim.account_num is null;
""")

# Для yasa_dwh_dim_clients_hist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_clients_hist (
        client_id,
        last_name,
        first_name,
        patronymic,
        date_of_birth,
        passport_num,
        passport_valid_to,
        phone,
        start_dt,
        end_dt,
        deleted_flg)
    SELECT
        stg.client_id,
        stg.last_name,
        stg.first_name,
        stg.patronymic,
        stg.date_of_birth,
        stg.passport_num,
        stg.passport_valid_to,
        stg.phone,
        stg.create_dt,
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'N'
    FROM de11an.yasa_stg_clients stg
    LEFT JOIN de11an.yasa_dwh_dim_clients_hist dim
        ON stg.client_id = dim.client_id
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
    WHERE dim.client_id is null;
""")

# Для yasa_dwh_fact_transactions
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_fact_transactions (
    trans_id,
    trans_date,
    amt,
    card_num,
    oper_type,
    oper_result,
    terminal)
        SELECT
            trans_id,
            trans_date,
            amt,
            card_num,
            oper_type,
            oper_result,
            terminal
        FROM de11an.yasa_stg_transactions;
 """)

# Для yasa_dwh_fact_passport_blacklist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_fact_passport_blacklist (enty_dt, passport_num)
    SELECT enty_dt, passport_num
    FROM de11an.yasa_stg_blacklist;
 """)

# Для yasa_dwh_dim_terminals_hist
cursor_tgt.execute(""" INSERT INTO de11an.yasa_dwh_dim_terminals_hist(
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    start_dt,
    end_dt,
    deleted_flg,
    processed_dt
)
SELECT DISTINCT
    stg.terminal_id,
    stg.terminal_type,
    stg.terminal_city,
    stg.terminal_address,
    stg.update_dt as start_dt,
    to_date('9999-12-31', 'YYYY-MM-DD') as end_dt,
    'N' as deleted_flg,
    NOW() as processed_dt
FROM de11an.yasa_stg_terminals stg
LEFT JOIN de11an.yasa_dwh_dim_terminals_hist tgt
ON stg.terminal_id = tgt.terminal_id
WHERE tgt.terminal_id is null;
""")

# 5.  Обновление в приемнике "обновлений" на источнике (формат SCD2).##########################################################

# Для yasa_dwh_dim_cards_hist
cursor_tgt.execute("""UPDATE de11an.yasa_dwh_dim_cards_hist
    SET
        end_dt = tmp.update_dt - interval '1 second'

    FROM
        (SELECT
            stg.card_num,
            stg.update_dt
        FROM de11an.yasa_stg_cards stg
        LEFT JOIN de11an.yasa_dwh_dim_cards_hist dim
            ON stg.card_num = dim.card_num
                and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                and dim.deleted_flg = 'N'
        WHERE stg.account_num <> dim.account_num or (stg.account_num is null and dim.account_num is not null) or (stg.account_num is not null and dim.account_num is null)
        )tmp
""")
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_cards_hist (card_num, account_num, start_dt, end_dt, deleted_flg)
    SELECT DISTINCT
        stg.card_num,
        stg.account_num,
        stg.update_dt,
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'N'
    FROM de11an.yasa_stg_cards stg
    LEFT JOIN de11an.yasa_dwh_dim_cards_hist dim
        ON stg.card_num = dim.card_num
            and dim.deleted_flg = 'N'
    WHERE stg.account_num <> dim.account_num or
            (stg.account_num is null and dim.account_num is not null) or
            (stg.account_num is not null and dim.account_num is null);
""")

# Для yasa_dwh_dim_accounts_hist
cursor_tgt.execute("""UPDATE de11an.yasa_dwh_dim_accounts_hist
    SET
        end_dt = tmp.update_dt - interval '1 second'
    FROM
        (SELECT
            stg.account_num,
            stg.update_dt
        FROM de11an.yasa_stg_accounts stg
        INNER JOIN de11an.yasa_dwh_dim_accounts_hist dim
            ON stg.account_num = dim.account_num
                and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                and dim.deleted_flg = 'N'
        WHERE stg.valid_to <> dim.valid_to or  (stg.valid_to is null and dim.valid_to is not null) or (stg.valid_to is not null and dim.valid_to is null) or
              stg.client <> dim.client or  (stg.client is null and dim.client is not null) or (stg.client is not null and dim.client is null)
        )tmp
    WHERE yasa_dwh_dim_accounts_hist.account_num = tmp.account_num;
""")
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_accounts_hist (account_num, valid_to, client, start_dt, end_dt, deleted_flg)
    SELECT DISTINCT
        stg.account_num,
        stg.valid_to,
        stg.client,
        stg.update_dt,
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'N'
    FROM de11an.yasa_stg_accounts stg
    LEFT JOIN de11an.yasa_dwh_dim_accounts_hist dim
        ON stg.account_num = dim.account_num
            and dim.deleted_flg = 'N'
    WHERE stg.valid_to <> dim.valid_to or  (stg.valid_to is null and dim.valid_to is not null) or (stg.valid_to is not null and dim.valid_to is null) or
          stg.client <> dim.client or  (stg.client is null and dim.client is not null) or (stg.client is not null and dim.client is null);
""")

# Для yasa_dwh_dim_clients_hist
cursor_tgt.execute("""UPDATE de11an.yasa_dwh_dim_clients_hist
    SET
        end_dt = tmp.update_dt - interval '1 second'
    FROM
        (SELECT
            stg.client_id,
            stg.update_dt
        FROM de11an.yasa_stg_clients stg
        INNER JOIN de11an.yasa_dwh_dim_clients_hist dim
            ON stg.client_id = dim.client_id
                and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                and dim.deleted_flg = 'N'
        WHERE stg.last_name <> dim.last_name or (stg.last_name is null and dim.last_name is not null) or (stg.last_name is not null and dim.last_name is null) or
            stg.first_name <> dim.first_name or (stg.first_name is null and dim.first_name is not null) or (stg.first_name is not null and dim.first_name is null) or
            stg.patronymic <> dim.patronymic or (stg.patronymic is null and dim.patronymic is not null) or (stg.patronymic is not null and dim.patronymic is null) or
            stg.date_of_birth <> dim.date_of_birth or (stg.date_of_birth is null and dim.date_of_birth is not null) or (stg.date_of_birth is not null and dim.date_of_birth is null) or
            stg.passport_num <> dim.passport_num or (stg.passport_num is null and dim.passport_num is not null) or (stg.passport_num is not null and dim.passport_num is null) or
            stg.passport_valid_to <> dim.passport_valid_to or (stg.passport_valid_to is null and dim.passport_valid_to is not null) or (stg.passport_valid_to is not null and dim.passport_valid_to is null) or
            stg.phone <> dim.phone or (stg.phone is null and dim.phone is not null) or (stg.phone is not null and dim.phone is null)
        )tmp
    WHERE yasa_dwh_dim_clients_hist.client_id = tmp.client_id;
""")
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_clients_hist (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            start_dt,
            end_dt,
            deleted_flg)
        SELECT DISTINCT
            stg.client_id,
            stg.last_name,
            stg.first_name,
            stg.patronymic,
            stg.date_of_birth,
            stg.passport_num,
            stg.passport_valid_to,
            stg.phone,
            stg.update_dt,
            to_date('9999-12-31', 'YYYY-MM-DD'),
            'N'
        FROM de11an.yasa_stg_clients stg
        LEFT JOIN de11an.yasa_dwh_dim_clients_hist dim
            ON stg.client_id = dim.client_id
                and dim.deleted_flg = 'N'
        WHERE stg.last_name <> dim.last_name or (stg.last_name is null and dim.last_name is not null) or (stg.last_name is not null and dim.last_name is null) or
            stg.first_name <> dim.first_name or (stg.first_name is null and dim.first_name is not null) or (stg.first_name is not null and dim.first_name is null) or
            stg.patronymic <> dim.patronymic or (stg.patronymic is null and dim.patronymic is not null) or (stg.patronymic is not null and dim.patronymic is null) or
            stg.date_of_birth <> dim.date_of_birth or (stg.date_of_birth is null and dim.date_of_birth is not null) or (stg.date_of_birth is not null and dim.date_of_birth is null) or
            stg.passport_num <> dim.passport_num or (stg.passport_num is null and dim.passport_num is not null) or (stg.passport_num is not null and dim.passport_num is null) or
            stg.passport_valid_to <> dim.passport_valid_to or (stg.passport_valid_to is null and dim.passport_valid_to is not null) or (stg.passport_valid_to is not null and dim.passport_valid_to is null) or
            stg.phone <> dim.phone or (stg.phone is null and dim.phone is not null) or (stg.phone is not null and dim.phone is null)
""")

# Для yasa_dwh_dim_terminals_hist
cursor_tgt.execute(f""" UPDATE de11an.yasa_dwh_dim_terminals_hist d
    SET
        end_dt = to_date('{terminals_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') - interval '1 second',
        processed_dt = tmp.processed_dt
    FROM  (
            SELECT
                stg.terminal_id,
                stg.terminal_type,
                stg.terminal_city,
                stg.terminal_address,
                now() as processed_dt
            FROM de11an.yasa_stg_terminals stg
            LEFT JOIN de11an.yasa_dwh_dim_terminals_hist tgt
            ON 1=1
                and stg.terminal_type = tgt.terminal_type
                and stg.terminal_city = tgt.terminal_city
                and stg.terminal_address = tgt.terminal_address
                and tgt.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                and tgt.deleted_flg = 'N'
            WHERE 1=0
                or stg.terminal_id <> tgt.terminal_id or (stg.terminal_id is null and tgt.terminal_id is not null) or (stg.terminal_id is not null and tgt.terminal_id is null)
                or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
                or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
                or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)
        ) tmp
    WHERE 1=1
        and d.terminal_id = tmp.terminal_id
        and d.terminal_type = tmp.terminal_type
        and d.terminal_city = tmp.terminal_city
""")
cursor_tgt.execute(f"""INSERT INTO de11an.yasa_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    start_dt,
    end_dt,
    deleted_flg,
    processed_dt
    )
    SELECT DISTINCT
        tgt.terminal_id,
        stg.terminal_type,
        stg.terminal_city,
        stg.terminal_address,
        to_date('{terminals_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as start_dt,
        to_date('9999-12-31', 'YYYY-MM-DD') as end_dt,
        'N' as deleted_flg,
        now() as processed_dt
    FROM
        de11an.yasa_stg_terminals stg
    INNER JOIN
        de11an.yasa_dwh_dim_terminals_hist tgt
    ON  stg.terminal_id = tgt.terminal_id
    WHERE 1=0
        or stg.terminal_type <> tgt.terminal_type or (stg.terminal_type is null and tgt.terminal_type is not null) or (stg.terminal_type is not null and tgt.terminal_type is null)
        or stg.terminal_city <> tgt.terminal_city or (stg.terminal_city is null and tgt.terminal_city is not null) or (stg.terminal_city is not null and tgt.terminal_city is null)
        or stg.terminal_address <> tgt.terminal_address or (stg.terminal_address is null and tgt.terminal_address is not null) or (stg.terminal_address is not null and tgt.terminal_address is null)
""")

# 6. Удаление в приемнике удаленных в источнике записей (формат SCD2).###########################################################

# Для yasa_dwh_dim_cards_hist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_cards_hist (card_num, account_num, start_dt, end_dt, deleted_flg)
    SELECT
        dim.card_num,
        dim.account_num,
        now(),
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'Y'
    FROM de11an.yasa_dwh_dim_cards_hist dim
    WHERE dim.card_num in (
        SELECT dim.card_num
        FROM de11an.yasa_dwh_dim_cards_hist dim
        LEFT JOIN de11an.yasa_stg_cards stg
            ON dim.card_num = stg.card_num
        WHERE stg.card_num is null
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
        )
    and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
    and dim.deleted_flg = 'N';
""")
cursor_tgt.execute("""UPDATE de11an.yasa_dwh_dim_cards_hist dim
    SET end_dt = now() - interval '1 second'
    WHERE dim.card_num in (
        SELECT dim.card_num
        FROM de11an.yasa_dwh_dim_cards_hist dim
        LEFT JOIN de11an.yasa_stg_cards stg
            ON dim.card_num = stg.card_num
        WHERE stg.card_num is null
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
        )
    and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
    and dim.deleted_flg = 'N';
""")

# Для yasa_dwh_dim_accounts_hist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_accounts_hist (account_num, valid_to, client, start_dt, end_dt, deleted_flg)
    SELECT
        dim.account_num,
        dim.valid_to,
        dim.client,
        now(),
        to_date('9999-12-31', 'YYYY-MM-DD'),
        'Y'
    FROM de11an.yasa_dwh_dim_accounts_hist dim
    WHERE dim.account_num in (
        SELECT dim.account_num
        FROM de11an.yasa_dwh_dim_accounts_hist dim
        LEFT JOIN de11an.yasa_stg_accounts stg
            ON dim.account_num = stg.account_num
        WHERE stg.account_num is null
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
        )
    and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
    and dim.deleted_flg = 'N';
""")
cursor_tgt.execute("""UPDATE de11an.yasa_dwh_dim_accounts_hist dim
    SET end_dt = now() - interval '1 second'
    WHERE dim.account_num in (
        SELECT dim.account_num
        FROM de11an.yasa_dwh_dim_accounts_hist dim
        LEFT JOIN de11an.yasa_stg_accounts stg
            ON dim.account_num = stg.account_num
        WHERE stg.account_num is null
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
        )
    and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
    and dim.deleted_flg = 'N';
""")

# Для yasa_dwh_dim_clients_hist
cursor_tgt.execute("""INSERT INTO de11an.yasa_dwh_dim_clients_hist (
            client_id,
            last_name,
            first_name,
            patronymic,
            date_of_birth,
            passport_num,
            passport_valid_to,
            phone,
            start_dt,
            end_dt,
            deleted_flg)
        SELECT
            dim.client_id,
            dim.last_name,
            dim.first_name,
            dim.patronymic,
            dim.date_of_birth,
            dim.passport_num,
            dim.passport_valid_to,
            dim.phone,
            now(),
            to_date('9999-12-31', 'YYYY-MM-DD'),
            'Y'
        FROM de11an.yasa_dwh_dim_clients_hist dim
        WHERE dim.client_id in (
            SELECT dim.client_id
            FROM de11an.yasa_dwh_dim_clients_hist dim
            LEFT JOIN de11an.yasa_stg_clients stg
                ON dim.client_id = stg.client_id
            WHERE stg.client_id is null
                and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                and dim.deleted_flg = 'N'
            )
        and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
        and dim.deleted_flg = 'N';
""")
cursor_tgt.execute("""UPDATE de11an.yasa_dwh_dim_clients_hist dim
    SET end_dt = now() - interval '1 second'
    WHERE dim.client_id in (
        SELECT dim.client_id
        FROM de11an.yasa_dwh_dim_clients_hist dim
        LEFT JOIN de11an.yasa_stg_clients stg
            ON dim.client_id = stg.client_id
        WHERE stg.client_id is null
            and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
            and dim.deleted_flg = 'N'
        )
    and dim.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
    and dim.deleted_flg = 'N';
""")

# Для yasa_dwh_dim_terminals_hist
cursor_tgt.execute(f""" INSERT INTO de11an.yasa_dwh_dim_terminals_hist (
    terminal_id,
    terminal_type,
    terminal_city,
    terminal_address,
    start_dt,
    end_dt,
    deleted_flg,
    processed_dt
)
    SELECT DISTINCT
        tgt.terminal_id,
        tgt.terminal_type,
        tgt.terminal_city,
        tgt.terminal_address,
        to_date('{terminals_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as start_dt,
        to_date('9999-12-31', 'YYYY-MM-DD') as end_dt,
        'Y' as deleted_flg,
        now() as processed_dt
    FROM
        de11an.yasa_dwh_dim_terminals_hist tgt
    WHERE
        tgt.terminal_id in (
            SELECT
                tgt2.terminal_id
            FROM
                de11an.yasa_dwh_dim_terminals_hist tgt2
            LEFT JOIN de11an.yasa_stg_terminals_del stg
                ON tgt2.terminal_id = stg.terminal_id
            WHERE stg.terminal_id is null
                and tgt2.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
                and tgt2.deleted_flg = 'N'
        )
        and tgt.end_dt = to_date('9999-12-31', 'YYYY-MM-DD')
        and tgt.deleted_flg = 'N'
""")
cursor_tgt.execute(f""" UPDATE de11an.yasa_dwh_dim_terminals_hist tgt
    SET
        end_dt = to_date('{terminals_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') - interval '1 second',
        processed_dt = now()
    WHERE tgt.terminal_id in (
        SELECT
            tgt2.terminal_id
        FROM
            de11an.yasa_dwh_dim_terminals_hist tgt2
        LEFT JOIN de11an.yasa_stg_terminals_del stg
            ON tgt2.terminal_id = stg.terminal_id
        WHERE stg.terminal_id is null
            and tgt2.end_dt = to_date( '9999-12-31', 'YYYY-MM-DD' )
            and tgt2.deleted_flg = 'N'
    )
    and tgt.end_dt = to_date( '9999-12-31', 'YYYY-MM-DD' )
    and tgt.deleted_flg = 'N'
""")

# 7. Обновление метаданных.###############################################################################################

cursor_tgt.execute("""UPDATE de11an.yasa_meta
    SET max_update_dt = COALESCE(
        (SELECT MAX(update_dt) FROM de11an.yasa_stg_cards),
        (SELECT max_update_dt FROM de11an.yasa_meta
        WHERE schema_name = 'de11an' and source_name = 'cards')
    )
""")

cursor_tgt.execute("""UPDATE de11an.yasa_meta
    SET max_update_dt = COALESCE(
        (SELECT MAX(update_dt) FROM de11an.yasa_stg_accounts),
        (SELECT max_update_dt FROM de11an.yasa_meta
        WHERE schema_name = 'de11an' and source_name = 'accounts')
    )
""")

cursor_tgt.execute("""UPDATE de11an.yasa_meta
    SET max_update_dt = COALESCE(
        (SELECT MAX(update_dt) FROM de11an.yasa_stg_clients),
        (SELECT max_update_dt FROM de11an.yasa_meta
        WHERE schema_name = 'de11an' and source_name = 'clients')
    )
""")

cursor_tgt.execute(f"""
    UPDATE de11an.yasa_meta
    SET
        max_update_dt = to_date('{transactions_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
    WHERE schema_name = 'de11an' and source_name = 'transactions'
""")

cursor_tgt.execute(f"""
    UPDATE de11an.yasa_meta
    SET
        max_update_dt = to_date('{blacklist_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
    WHERE schema_name = 'de11an' and source_name = 'blacklist'
""")

cursor_tgt.execute(f"""
    UPDATE de11an.yasa_meta
    SET
        max_update_dt = to_date('{terminals_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
    WHERE schema_name = 'de11an' and source_name = 'terminals'
""")

# Перемещение обработанных файлов в архив.########################################################################################

shutil.move(
    f'{TRANSACTION_FILES_PREFIX}/{transactions_file}',
    f'{ARCHIVE_FILES_PREFIX}/{transactions_file}.backup'
)

shutil.move(
    f'{BLACKLIST_FILES_PREFIX}/{blacklist_file}',
    f'{ARCHIVE_FILES_PREFIX}/{blacklist_file}.backup'
)

shutil.move(
    f'{TERMINAL_FILES_PREFIX}/{terminals_file}',
    f'{ARCHIVE_FILES_PREFIX}/{terminals_file}.backup'
)

# # Загрузка данных в yasa_rep_fraud.######################################################################################

## Report 1. Совершение операции при просроченном или заблокированном паспорте.
cursor_tgt.execute(f"""
    WITH info_client AS (
    SELECT
        acnt.account_num,
        acnt.valid_to,
        acnt.client,
        (cln.last_name || ' ' || cln.first_name || ' ' || cln.patronymic) as fio,
        cln.passport_num,
        cln.passport_valid_to,
        cln.phone
    FROM
        de11an.yasa_stg_accounts acnt
    INNER JOIN de11an.yasa_stg_clients cln
        ON acnt.client = cln.client_id
), info_trans AS (
    SELECT
        tr.trans_date,
        tr.card_num,
        ysc.card_num,
        ysc.account_num
    FROM
        de11an.yasa_dwh_fact_transactions tr
    INNER JOIN de11an.yasa_stg_cards ysc
        ON     trim(tr.card_num)  = trim(ysc.card_num)
    WHERE tr.oper_result = 'SUCCESS' and  tr.trans_date > to_date('{transactions_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')
)
SELECT DISTINCT
    tr.trans_date,
    info_client.passport_num,
    info_client.fio,
    info_client.phone,
    1 as event_type,
    to_date('{transactions_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as report_dt
FROM
    info_client
INNER JOIN info_trans tr
    ON info_client.account_num = tr.account_num
WHERE info_client.passport_num  in (
    SELECT
        passport_num
    FROM
        de11an.yasa_dwh_fact_passport_blacklist blt
        )  or info_client.passport_valid_to < tr.trans_date
ORDER BY info_client.fio;
""")

## Report 2. Совершение операции при недействующем договоре.
cursor_tgt.execute(f"""INSERT INTO de11an.yasa_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
WITH info_client AS (
    SELECT
        acnt.account_num,
        acnt.valid_to,
        acnt.client,
        (cln.last_name || ' ' || cln.first_name || ' ' || cln.patronymic) as fio,
        cln.passport_num,
        cln.passport_valid_to,
        cln.phone
    FROM
        de11an.yasa_stg_accounts acnt
    INNER JOIN de11an.yasa_stg_clients cln
        ON acnt.client = cln.client_id
), info_trans AS (
    SELECT
        tr.trans_date,
        tr.card_num,
        ysc.card_num,
        ysc.account_num
    FROM
        de11an.yasa_dwh_fact_transactions tr
    INNER JOIN de11an.yasa_stg_cards ysc
        ON     trim(tr.card_num)  = trim(ysc.card_num)
    WHERE tr.oper_result = 'SUCCESS'
)
SELECT DISTINCT
    tr.trans_date,
    info_client.passport_num,
    info_client.fio,
    info_client.phone,
    2 as event_type,
    to_date('{transactions_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as report_dt
FROM
    info_client
INNER JOIN info_trans tr
    ON info_client.account_num = tr.account_num
WHERE trans_date > info_client.valid_to;
 """)

## Report 3. Совершение операций в разных городах в течение одного часа.
cursor_tgt.execute(f"""
    INSERT INTO de11an.yasa_rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
WITH info_cte AS (
    SELECT DISTINCT
        yddch2.client_id,
        (yddch2.last_name || ' ' || yddch2.first_name || ' ' || yddch2.patronymic) as fio,
        yddch2.passport_num,
        yddch2.phone,
        yst.terminal_id,
        yst.terminal_city,
        yddch.card_num,
        yddch.account_num,
        yddah.account_num,
        yddah.client,
        ydft.trans_date,
        ydft.card_num,
        ydft.terminal,
        ydft.oper_result,
        ydft.trans_id,
        lead(ydft.trans_id) over (partition by ydft.card_num order by ydft.trans_date) as lead_tran,
        lead(yst.terminal_city) over (partition by ydft.card_num order by ydft.trans_date) as lead_city,
        lead(ydft.trans_date) over (partition by ydft.card_num order by ydft.trans_date) as lead_date
    FROM
        de11an.yasa_dwh_fact_transactions ydft
    LEFT JOIN de11an.yasa_stg_terminals yst
        ON ydft.terminal = yst.terminal_id
    LEFT JOIN de11an.yasa_dwh_dim_cards_hist yddch
        ON trim(ydft.card_num) = trim(yddch.card_num)
    LEFT JOIN de11an.yasa_dwh_dim_accounts_hist yddah
        ON yddch.account_num = yddah.account_num
    LEFT JOIN de11an.yasa_dwh_dim_clients_hist yddch2
        ON yddah.client = yddch2.client_id
)
    SELECT DISTINCT
        max(trans_date),
        info_cte.passport_num,
        info_cte.fio,
        info_cte.phone,
        3 as event_type,
        to_date('{transactions_file_dt.strftime('%Y-%m-%d')}', 'YYYY-MM-DD') as report_dt
    FROM info_cte
    WHERE terminal_city <> lead_city and EXTRACT (MINUTE FROM lead_date - trans_date) < 60 and oper_result = 'SUCCESS'
    GROUP BY info_cte.passport_num, info_cte.fio, info_cte.phone;
 """)

conn_tgt.commit()

cursor_src.close()
cursor_tgt.close()
conn_src.close()
conn_tgt.close()
