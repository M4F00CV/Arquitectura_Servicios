import pika, sys, os

def main():
    # 1. Conexión
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # 2. Declaramos la cola (buena práctica hacerlo en ambos lados)
    channel.queue_declare(queue='hola')

    # 3. Definimos qué hacer cuando llega un mensaje (callback)
    def callback(ch, method, properties, body):
        print(f" [x] Recibido: {body.decode()}")

    # 4. Configuramos el consumo
    channel.basic_consume(queue='hola',
                          on_message_callback=callback,
                          auto_ack=True)

    print(' [*] Esperando mensajes. Presiona CTRL+C para salir.')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrumpido')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
