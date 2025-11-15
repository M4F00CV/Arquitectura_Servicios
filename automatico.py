import pika
import sys
import random
import time
import xml.etree.ElementTree as ET

QUEUE_NAME = 'cola_numeros'

def main():
    # 1. Validación de entrada: Aseguramos que el usuario ponga un número
    if len(sys.argv) != 2:
        print("   Error de uso.")
        print("   Forma correcta: python automatico.py <cantidad>")
        print("   Ejemplo: python automatico.py 10")
        sys.exit(1)

    try:
        # Convertimos el argumento de texto a entero
        cantidad_a_enviar = int(sys.argv[1])
    except ValueError:
        print("   Error: El argumento debe ser un número entero.")
        sys.exit(1)

    # 2. Conexión a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    print(f" [*] Iniciando el envío de {cantidad_a_enviar} operaciones...")

    # 3. Bucle determinado: Se ejecutará exactamente 'cantidad_a_enviar' veces
    for i in range(cantidad_a_enviar):
        
        numeros = [random.randint(1, 1000) for _ in range(3)]
        
        # --- Construcción del XML ---
        root = ET.Element('datos')
        for n in numeros:
            child = ET.SubElement(root, 'num')
            child.text = str(n)
        
        mensaje_xml = ET.tostring(root)
        # ----------------------------

        channel.basic_publish(exchange='',
                              routing_key=QUEUE_NAME,
                              body=mensaje_xml)
        
        # Feedback visual con contador (ej: 1/10, 2/10...)
        print(f" [x] ({i+1}/{cantidad_a_enviar}) Enviado: {numeros}")
        
        # Pequeña pausa (50ms) para ver fluir los logs en las otras terminales
        time.sleep(0.01)

    # 4. Cierre limpio al terminar el bucle
    print(" [ok] Lote finalizado. Cerrando productor.")
    connection.close()

if __name__ == '__main__':
    main()
