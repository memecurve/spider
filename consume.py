import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost', port=5672))
channel = connection.channel()

#channel.queue_declare(queue='download-tasks')

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)

channel.basic_consume(callback,
                      queue='download-tasks',
                      no_ack=True)

channel.start_consuming()
