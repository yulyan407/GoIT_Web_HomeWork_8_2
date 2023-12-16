import json
import os
import sys
from producer import Contact

from mongoengine import connect
import pika


connect(
    db="web17",
    host="mongodb+srv://userweb17:789123@cluster0.fu4oomv.mongodb.net/?retryWrites=true&w=majority",
)


def main():
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='send_email')

    def callback(ch, method, properties, body):
        message = body.decode('utf-8')
        message_dict = json.loads(message)
        contact_id = message_dict['contact_id']

        contact = Contact.objects(id=contact_id).first()

        if contact:
            contact.email_sent = True
            contact.save()
            print(f"Contact {contact.full_name} message sent successfully.")
        else:
            print(f"Contact with ID {contact_id} not found.")

    channel.basic_consume(queue='send_email', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)