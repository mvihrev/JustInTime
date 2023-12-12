import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import smtplib
from email.mime.text import MIMEText
import json


def send_notification_email(product_name, stock_quantity, rop, eoq, need_to_order):
    sender_email = "just.in.time.company@yandex.ru"
    password = "SOME_PASSWORD"
    receiver_email = "user@gmail.com"
    subject = f"Внимание, возможна нехватка товара: {product_name}"
    body = (f"Текущее количество: {stock_quantity}\nЗначение ROP: {rop}\nЗначение EOQ: {eoq}\n"
            f"Необходимо заказать {need_to_order}")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email

    server = smtplib.SMTP_SSL('smtp.yandex.ru:465')
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, msg.as_string())

conn = psycopg2.connect(
    database="just_in_time",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

listen_command = "LISTEN email_channel;"

while True:
    with conn.cursor() as cursor:
        cursor.execute(listen_command)
        result = conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            data = json.loads(notify.payload)
            product_name = data['product_name']
            stock_quantity = data['stock_quantity']
            rop = data['rop']
            eoq = data['eoq']
            need_to_order = eoq - stock_quantity
            send_notification_email(product_name, stock_quantity, rop, eoq, need_to_order)



