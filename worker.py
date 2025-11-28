import pika
import json
import time
import sys
import os
from config import *

class Worker:
    def __init__(self, worker_id):
        self.worker_id = worker_id # ID del worker
        self.modelo = None # Modelo cargado
        self.connection = None # Conexion
        self.channel = None # Canal
        self.escenarios_procesados = 0 # Numero de escenarios procesados
        self.esperando_modelo = True # Bandera para esperar el modelo
        
    # Conectarse a RabbitMQ
    def conectar(self):
        creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        
        # Declarar solo colas de escenarios y resultados
        # NO declarar cola de modelo (la crea el productor con TTL)
        self.channel.queue_declare(queue=QUEUE_ESCENARIOS, durable=True)
        self.channel.queue_declare(queue=QUEUE_RESULTADOS, durable=True)
        
        # Configurar QoS para procesar un mensaje a la vez
        self.channel.basic_qos(prefetch_count=1)
        
        print(f"[EXITO] Worker {self.worker_id} conectado a RabbitMQ")
    
    # El modelo se lee una sola vez de la cola de modelos
    def leer_modelo(self):
        print(f"[*] Worker {self.worker_id} esperando modelo...")
        
        try:
            # Se usa get() para que el worker pregunte si hay mensaje (modelo) en la cola
            # En caso de que haya modelo, se envia al worker
            method_frame, header_frame, body = self.channel.basic_get(queue=QUEUE_MODELO, auto_ack=False)
            
            if method_frame:
                try:
                    self.modelo = json.loads(body.decode('utf-8')) # Cargamos modelo
                    
                    # Devolvemos a la cola por medio de un nack para conservar TTL
                    # No lo consume, por lo que otro worker puede consultar el modelo
                    # Solo se hace una vez
                    self.channel.basic_nack(
                        delivery_tag=method_frame.delivery_tag,
                        requeue=True  # El mensaje vuelve con su TTL original
                    )
                    
                    print(f"[EXITO] Worker {self.worker_id} - Modelo recibido: {self.modelo.get('nombre', 'N/A')}")
                    print(f"    Fórmula: {self.modelo['formula']}")
                    print(f"    Variables: {list(self.modelo['variables'].keys())}")
                    self.esperando_modelo = False # Ya tiene modelo cargado
                    return True
                except Exception as e:
                    print(f"[ERROR] Worker {self.worker_id} - Error al leer modelo: {e}")
                    if method_frame:
                        self.channel.basic_nack(delivery_tag=method_frame.delivery_tag, requeue=True)
                    return False
            else:
                return False
        except Exception as e:
            print(f"[ERROR] Worker {self.worker_id} - Error al consultar cola de modelo: {e}")
            return False
    
    # Evalua el modelo con los valores del escenario
    def evaluar_modelo(self, escenario):
        try:
            # Crear contexto de variables para eval
            contexto = escenario.copy()
            
            # Evaluar la formula (sin funciones internas de Python)
            resultado = eval(self.modelo['formula'], {"__builtins__": {}}, contexto)
            
            return resultado
        except Exception as e:
            print(f"[ERROR] Worker {self.worker_id} - Error al evaluar: {e}")
            return None
    
    # Procesa escenarios de la cola
    def procesar_escenarios(self):
        print(f"[*] Worker {self.worker_id} procesando escenarios...\n")
        
        def callback(ch, method, props, body):
            try:
                # Decodificar escenario
                escenario = json.loads(body.decode('utf-8'))
                
                # VERIFICAR: Las variables del escenario coinciden con el modelo?
                variables_escenario = set(escenario.keys())
                variables_modelo = set(self.modelo['variables'].keys())
                
                if variables_escenario != variables_modelo:
                    # Modelo cambio, necesitamos recargar
                    print(f"\n[ADVERTENCIA] Worker {self.worker_id} - Detectado cambio de modelo")
                    print(f"    Variables esperadas: {variables_modelo}")
                    print(f"    Variables recibidas: {variables_escenario}")
                    print(f"[*] Recargando modelo...\n")
                    
                    # Recargar modelo
                    if self.leer_modelo():
                        print(f"[EXITO] Worker {self.worker_id} - Modelo actualizado: {self.modelo['nombre']}")
                        # Reiniciar contador de escenarios procesados
                        self.escenarios_procesados = 0
                    else:
                        print(f"[ADVERTENCIA] Worker {self.worker_id} - No se pudo recargar modelo")
                        # Rechazar mensaje y continuar
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                        return
                
                # Evaluar modelo
                resultado = self.evaluar_modelo(escenario)
                
                if resultado is not None:
                    # Preparar resultado completo
                    resultado_completo = {
                        "worker_id": self.worker_id,
                        "escenario": escenario,
                        "resultado": round(resultado, 4),
                        "timestamp": time.time(),
                        "modelo": self.modelo.get('nombre', 'N/A')
                    }
                    
                    # Publicar resultado
                    ch.basic_publish(
                        exchange='',
                        routing_key=QUEUE_RESULTADOS,
                        body=json.dumps(resultado_completo).encode('utf-8'),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    
                    self.escenarios_procesados += 1
                    
                    # Mostrar progreso cada 10 escenarios
                    if self.escenarios_procesados % 10 == 0:
                        print(f"[W{self.worker_id}] Procesados: {self.escenarios_procesados} | Último resultado: {resultado:.4f}")
                
                # Confirmar procesamiento
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except Exception as e:
                print(f"[ERROR] Worker {self.worker_id}: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Consumir escenarios
        self.channel.basic_consume(
            queue=QUEUE_ESCENARIOS,
            on_message_callback=callback,
            auto_ack=False
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
    
    # Cerrar la conexion
    def cerrar(self):
        if self.connection:
            self.connection.close()

def main():
    if len(sys.argv) < 2:
        print("Uso: python worker.py <worker_id>")
        print("\nEjemplo: python worker.py 1")
        sys.exit(1)
    
    worker_id = sys.argv[1]
    worker = Worker(worker_id) # Creamos al worker
    
    try:
        print("=" * 50)
        print(f" WORKER {worker_id} - Simulación Montecarlo")
        print("=" * 50)
        
        # Conectar
        worker.conectar()
        
        # Intentar leer modelo
        max_intentos = 10
        intentos = 0
        
        print(f"[*] Worker {worker_id} - Intentando leer modelo de la cola...")
        while worker.esperando_modelo and intentos < max_intentos: # Esperando a leer el modelo
            if worker.leer_modelo():
                break
            intentos += 1
            print(f"[*] Worker {worker_id} - Esperando modelo... (intento {intentos}/{max_intentos})")
            time.sleep(2)
        
        if worker.esperando_modelo: # Si despues de los intentos aun esta esperando modelo
            print(f"\n[!] Worker {worker_id} - No se encontró modelo disponible.")
            print(f"[*] Asegúrate de ejecutar primero:")
            print(f"    python productor.py <modelo.json>")
            print()
            sys.exit(1)
        
        # Procesar escenarios con el modelo leido
        print(f"[*] Worker {worker_id} - Comenzando a procesar escenarios...")
        worker.procesar_escenarios()
        
    except KeyboardInterrupt:
        print(f"\n\n{'=' * 50}")
        print(f" Worker {worker_id} - Detenido por usuario")
        print(f" Escenarios procesados: {worker.escenarios_procesados}")
        print(f"{'=' * 50}")
    except Exception as e:
        print(f"\n[ERROR] Worker {worker_id}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        worker.cerrar()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

if __name__ == "__main__":
    main()
