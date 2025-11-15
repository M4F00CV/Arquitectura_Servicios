import pika
import random
import time
import xml.etree.ElementTree as ET 

# Nombre de la cola donde enviaremos los datos
QUEUE_NAME = 'cola_numeros'

def main():
    # 1. CONEXIÓN: Nos conectamos al servidor RabbitMQ (localhost si es Docker en la misma máquina)
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # 2. DECLARACIÓN: Aseguramos que la cola exista. 
    # Si ya existe, no hace nada. Si no, la crea. Esto evita errores si el productor inicia antes.
    channel.queue_declare(queue=QUEUE_NAME)

    print(" [*] Productor XML iniciado...")

    try:
        while True:
            # Generamos 3 números aleatorios del 1 al 1000
            numeros = [random.randint(1, 1000) for _ in range(3)]
            
            # --- CONSTRUCCIÓN DEL XML ---
            # Creamos el elemento raíz: <datos>
            root = ET.Element('datos')           
            
            # Iteramos sobre los números para crear hijos: <num>123</num>
            for n in numeros:
                child = ET.SubElement(root, 'num') # Creamos la etiqueta hija
                child.text = str(n)                # Ponemos el número como texto dentro
            
            # SERIALIZACIÓN: Convertimos el objeto Python XML a bytes para poder enviarlo por red
            mensaje_xml = ET.tostring(root) 
            # -----------------------------

            # 3. PUBLICACIÓN: Enviamos el mensaje a la cola
            channel.basic_publish(exchange='',              # Intercambio por defecto (directo)
                                  routing_key=QUEUE_NAME,   # La cola destino
                                  body=mensaje_xml)         # El contenido (payload) en bytes
            
            print(f" [x] Enviado XML: {mensaje_xml.decode()}")
            
            # Pausa de 1 segundo para no saturar la CPU ni el log visualmente
            time.sleep(1)

    except KeyboardInterrupt:
        # Cierre limpio al presionar CTRL+C
        print("\nApagando productor...")
        connection.close()

if __name__ == '__main__':
    main()
