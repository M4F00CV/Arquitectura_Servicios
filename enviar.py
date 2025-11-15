import pika

# 1. Conexión al servidor RabbitMQ (que corre en Docker en localhost)
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Declaramos una cola llamada 'hola'
# (Si no existe, la crea. Si existe, no hace nada)
channel.queue_declare(queue='hola')

# 3. Publicamos el mensaje
mensaje = "¡Hola Mundo desde Debian!"
channel.basic_publish(exchange='',
                      routing_key='hola',
                      body=mensaje)

print(f" [x] Enviado: '{mensaje}'")

# 4. Cerramos la conexión
connection.close()

