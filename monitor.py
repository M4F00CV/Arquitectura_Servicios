import pika
import xml.etree.ElementTree as ET 

QUEUE_OUTPUT = 'cola_resultados'

# Diccionario global para almacenar estadísticas en memoria mientras corre el programa
stats = {'suma': 0, 'resta': 0, 'multi': 0}

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    # Declaramos la cola por si iniciamos el monitor antes que los workers
    channel.queue_declare(queue=QUEUE_OUTPUT)

    print(" [*] Monitor XML esperando resultados...")

    def callback(ch, method, properties, body):
        # --- LECTURA DE XML ---
        root = ET.fromstring(body)
        
        # Buscamos hijos directos por nombre de etiqueta ('tipo' y 'valor')
        tipo = root.find('tipo').text
        valor = root.find('valor').text
        # ----------------------

        # Actualizamos el contador si el tipo es válido
        if tipo in stats:
            stats[tipo] += 1
        
        # Imprimimos el reporte en consola
        print("------------------------------------------------")
        print(f" [XML Recibido] Operación: {tipo} | Resultado: {valor}")
        print(f" ESTADÍSTICAS: Sum({stats['suma']}) Rest({stats['resta']}) Multi({stats['multi']})")
        print("------------------------------------------------")

        # Confirmamos recepción (ACK) para borrar el mensaje de la cola
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=QUEUE_OUTPUT, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    main()
