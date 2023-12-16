import json
from random import randint
from mongoengine import connect, Document, StringField, BooleanField
from faker import Faker
import pika


connect(
    db="web17",
    host="mongodb+srv://userweb17:789123@cluster0.fu4oomv.mongodb.net/?retryWrites=true&w=majority",
)

fake = Faker('uk-UA')

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='send_email')


class Contact(Document):
    full_name = StringField(required=True)
    email = StringField(required=True)
    email_sent = BooleanField(default=False)
    phone = StringField(max_length=50)
    sms_sent = BooleanField(default=False)
    meta = {'collection': 'contacts'}


def create_tasks(num: int):
    for _ in range(num):
        full_name = fake.name()
        email = fake.email()
        phone = fake.phone_number()

        contact = Contact(full_name=full_name, email=email, phone=phone)
        contact.save()

        message = {'contact_id': str(contact.id)}
        channel.basic_publish(exchange='', routing_key='send_email', body=json.dumps(message))

        print(f"Contact '{full_name}' added to the queue")

    connection.close()


if __name__ == "__main__":
    create_tasks(randint(5, 20))
