import tinvest
from datetime import datetime
import os
from dotenv import load_dotenv
import json
import requests
import psycopg2
import logging
from tqdm import tqdm
from psycopg2.extras import LoggingConnection

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("loggerinformation")

load_dotenv()

TOKEN = os.getenv('TOKEN')
TABLE_NAME = os.getenv('TABLE_NAME', 'finkoff_invest')
figi_url = 'https://api-invest.tinkoff.ru/openapi/market/search/by-figi'
figi_headers = { "Authorization" : f"Bearer {TOKEN}" }

def get_conn():
    conn = psycopg2.connect(
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST', 'localhost'),
        port=os.getenv('PORT', '5432'),
        connect_timeout=os.getenv('CONNECT_TIMEOUT', '5'),
        connection_factory=LoggingConnection
    )
    conn.initialize(logger)
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (data json);")
    return conn

def save(operations):
    conn = get_conn()
    for operation in tqdm(operations, desc='export to db') :
        if 'id' not in operation: continue
        operation_id = int(operation['id'])
        date = operation['date'].strftime('%Y-%m-%d %H:%M:%S')
        currency = operation['currency']
        payment = float(operation['payment'])
        price = operation['price']
        quantity = operation['quantity']
        operation_type = operation.get('operation_type')
        instrument_type = operation['instrument_type']
        commission = operation.get('commission')
        if commission is not None:
            commission['value'] = float(commission['value'])
            commission = json.dumps(commission, indent = 1)
        status = operation['status']
        ticker = None
        name = None

        if operation['figi'] is not None:
            new_url = figi_url + f"?figi={operation['figi']}"
            resp = requests.get(url=new_url, headers=figi_headers)
            if resp.status_code == 200:
                data = resp.json()
                ticker = data['payload']['ticker']
                name = data['payload']['name']
        SQL = f"""INSERT INTO {TABLE_NAME} (operation_id, date, currency, payment, price, quantity, operation_type, instrument_type, commission, status, ticker, name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"""
        SQL_DATA = (operation_id, date, currency, payment, price, quantity,
                    operation_type, instrument_type, commission,
                    status, ticker, name)
        cursor = conn.cursor()
        cursor.execute(SQL, SQL_DATA)
        conn.commit()

def main():
    client = tinvest.SyncClient(TOKEN)
    try:
        from_ = datetime.fromisoformat(os.getenv('FROM'))
        to_ = datetime.fromisoformat(os.getenv('TO'))
        response = client.get_operations(from_, to_)
        save(response.dict()['payload']['operations'])
    except tinvest.BadRequestError as e:
        print(e.response)  # tinvest.Error


if __name__ == '__main__':
    main()


# now = datetime.now(tz=timezone('Europe/Moscow'))
# yesterday = now - timedelta(days=1800)
# ops = client.operations.operations_get(_from=yesterday.isoformat(), to=now.isoformat())
# for operation in ops.payload.operations:
#     sql = f"INSERT IGNORE INTO `tinkoff_invest` SET `operation_id` = {operation.id}"
#     sql += f", `date` = '{operation.date.strftime('%Y-%m-%d %H:%M:%S')}'"
#     sql += f", `currency` = '{operation.currency}'"
#     sql += f", `payment` = {float(operation.payment)}"
#     if operation.price is not None: sql += f", `price` = {operation.price}"
#     if operation.quantity is not None: sql += f", `quantity` = {operation.quantity}"
#     sql += f", `operation_type` = '{operation.operation_type}'"
#     if operation.instrument_type is not None: sql += f", `instrument_type` = '{operation.instrument_type}'"
#     if operation.commission is not None: sql += f", `commission_currency` = '{operation.commission.currency}'"
#     if operation.commission is not None: sql += f", `commission_value` = {float(operation.commission.value)}"
#     sql += f", `status` = '{operation.status}'"
#     if operation.figi is not None:
#         sql += f", `figi` = '{operation.figi}'"
#         new_url = url + f"?figi={operation.figi}"
#         resp = requests.get(url=new_url, headers=headers)
#         if resp.status_code == 200:
#             data = resp.json()
#             sql += f", `ticker` = '{data['payload']['ticker']}'"
#             sql += f", `name` = '{data['payload']['name']}'"
#     cursor = connection.cursor()
#     cursor.execute(sql)
#     print(sql.replace('INSERT IGNORE INTO `tinkoff_invest` SET `id` = ', ''))
