import pika
import sys
import xml.etree.ElementTree as ET # <--- Librería para manejar XML

# Definimos las colas de entrada (trabajo) y salida (resultados)
QUEUE_INPUT = 'cola_numeros'
QUEUE_OUTPUT = 'cola_resultados'

# --- Funciones de lógica de negocio (Matemáticas simples) ---
def operacion_suma(nums): return sum(nums)
def operacion_resta(nums): return nums[0] - nums[1] - nums[2]
def operacion_multi(nums): return nums[0] * nums[1] * nums[2]

def main():
    # Validación de argumentos al ejecutar el script
    if len(sys.argv) < 2:
        print("Uso: python chambeador.py [suma|resta|multi]")
        sys.exit(1)
    
    # Leemos qué tipo de worker es este (ej: 'suma')
    tipo_worker = sys.argv[1]

    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declaramos ambas colas para asegurarnos de que existen
    channel.queue_declare(queue=QUEUE_INPUT)
    channel.queue_declare(queue=QUEUE_OUTPUT)

    # QOS (Quality of Service): "Fair Dispatch"
    # Le dice a RabbitMQ: "No me des otro mensaje hasta que haya terminado (ACK) el actual".
    # Esto evita que un worker se sobrecargue mientras otros están libres.
    channel.basic_qos(prefetch_count=1)

    # Función CALLBACK: Se ejecuta cada vez que llega un mensaje
    def callback(ch, method, properties, body):
        
        # --- LECTURA DE XML (Deserialización) ---
        # Convertimos los bytes recibidos ('body') de vuelta a un objeto XML
        root = ET.fromstring(body)       
        # Buscamos todas las etiquetas <num>, extraemos su texto y lo convertimos a Entero
        numeros = [int(child.text) for child in root.findall('num')]
        # ----------------------------------------

        resultado = 0
        # Seleccionamos la operación según el tipo de worker
        if tipo_worker == 'suma': resultado = operacion_suma(numeros)
        elif tipo_worker == 'resta': resultado = operacion_resta(numeros)
        elif tipo_worker == 'multi': resultado = operacion_multi(numeros)

        # --- CONSTRUCCIÓN XML DE RESPUESTA ---
        # Estructura: <resultado><tipo>suma</tipo><valor>100</valor></resultado>
        res_root = ET.Element('resultado')
        
        etiqueta_tipo = ET.SubElement(res_root, 'tipo')
        etiqueta_tipo.text = tipo_worker
        
        etiqueta_valor = ET.SubElement(res_root, 'valor')
        etiqueta_valor.text = str(resultado) # Todo en XML debe ser texto (string)
        
        mensaje_respuesta = ET.tostring(res_root)
        # -------------------------------------

        # Publicamos el resultado en la otra cola ('cola_resultados')
        channel.basic_publish(exchange='', routing_key=QUEUE_OUTPUT, body=mensaje_respuesta)
        
        print(f" [x] {tipo_worker} procesó: {mensaje_respuesta.decode()}")
        
        # ACK: Confirmamos a RabbitMQ que el trabajo terminó.
        # Si no hacemos esto, el mensaje volverá a la cola cuando el worker se desconecte.
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Configuramos el consumidor para escuchar la cola de entrada
    channel.basic_consume(queue=QUEUE_INPUT, on_message_callback=callback)
    
    print(f" [*] Worker XML ({tipo_worker}) esperando...")
    # Bucle infinito de escucha
    channel.start_consuming()

if __name__ == '__main__':
    main()
