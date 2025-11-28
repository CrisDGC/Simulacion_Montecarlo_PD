#!/usr/bin/env python
# -*- coding: utf-8 -*-
# PRODUCTOR
# Descripcion: El productor lee en la cola COMANDOS
# En dicha cola, el dashboard envia solicitudes en la cual carga los diferentes modelos
# El productor lee cual modelo y lo publica en la cola MODELO
# Extrae las caracteristicas del modelo (variables) y genera escenarios aleatorios unicos
# Publica los escenarios en la cola ESCENARIO
# Los workers consumen el modelo de la cola de MODELOS una sola vez
# Los workers van consumiendo escenarios de la cola de ESCENARIOS

import pika
import json
import random
import time
import signal
import sys
import os
import threading
from pathlib import Path
import numpy as np
from config import (RABBIT_HOST, RABBIT_USER, RABBIT_PASS,
                   QUEUE_MODELO, QUEUE_ESCENARIOS, QUEUE_RESULTADOS,
                   QUEUE_COMANDOS, MODELO_TTL, ESCENARIO_INTERVAL)

class ProductorServicio:
    def __init__(self):
        self.connection = None # Establecer conexion
        self.channel = None # Canal de comunicacion
        self.modelo_actual = None # Modelo actual
        self.generando = False # Bandera para generar escenarios
        self.thread_generacion = None # Hilo que genera escenarios
        self.escenarios_generados = set()  # Para que no se repitan escenarios
        self.total_generados = 0 # Contador de escenarios
        # Conexión separada para el thread de generacion
        self.connection_generacion = None # Conexion del hilo para generar escenarios
        self.channel_generacion = None # Canal del hilo para generar escenarios
    
    # Establece conexion con RabbitMQ
    def conectar(self):
        creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS) # Credenciales
        params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds) # Parametros de conexion
        self.connection = pika.BlockingConnection(params) # Conexion bloqueante
        self.channel = self.connection.channel() # Canal de comunicacion
        
        # Declarar colas necesarias
        try:
            # Crear cola donde se deposita el modelo
            self.channel.queue_declare(
                queue=QUEUE_MODELO,
                durable=True, # Persistente
                arguments={'x-message-ttl': MODELO_TTL} # Modelo con TTL
            )
        except Exception as e:
            # Si ya existe, no la crea
            print(f"[ADVERTENCIA] Cola modelo ya existe: {e}")
            self.channel.queue_declare(queue=QUEUE_MODELO, durable=True)
        
        self.channel.queue_declare(queue=QUEUE_ESCENARIOS, durable=True) # Declara cola escenarios
        self.channel.queue_declare(queue=QUEUE_RESULTADOS, durable=True) # Declara cola resultados
        self.channel.queue_declare(queue=QUEUE_COMANDOS, durable=True) # Declara cola comandos
        
        print("[EXITO] Conexion establecida con RabbitMQ")
    
    # Carga un modelo desde un archivo JSON
    def cargar_modelo(self, ruta_archivo):
        try:
            # Cargar archivo JSON
            with open(ruta_archivo, 'r', encoding='utf-8') as f:
                modelo = json.load(f)
            
            # Validar estructura basica
            campos_requeridos = ['nombre', 'descripcion', 'formula', 'variables']
            for campo in campos_requeridos:
                if campo not in modelo:
                    raise ValueError(f"Modelo invalido: falta campo '{campo}'")
            
            print(f"[EXITO] Modelo cargado: {modelo['nombre']}")
            print(f"    Descripción: {modelo['descripcion']}")
            print(f"    Fórmula: {modelo['formula']}")
            return modelo
        
        except FileNotFoundError:
            print(f"[ERROR] Archivo no encontrado: {ruta_archivo}")
            return None
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error al parsear JSON: {e}")
            return None
        except Exception as e:
            print(f"[ERROR] Error al cargar modelo: {e}")
            return None
    
    # Publica el modelo en la cola de modelos
    def publicar_modelo(self):
        # Si no hay modelo actual
        if not self.modelo_actual:
            print("[ERROR] No hay modelo cargado")
            return False
        
        # Purgar colas anteriores para limpiar escenarios y modelo anterior
        try:
            self.channel.queue_purge(queue=QUEUE_MODELO)
            print(f"[ADVERTENCIA] Cola de modelo purgada")
        except:
            pass
        
        try:
            self.channel.queue_purge(queue=QUEUE_ESCENARIOS)
            print(f"[ADVERTENCIA] Cola de escenarios purgada (nuevo modelo)")
        except:
            pass
        
        # Publicar nuevo modelo
        body = json.dumps(self.modelo_actual) # Serializa a cadena con formato JSON
        self.channel.basic_publish(
            exchange='',
            routing_key=QUEUE_MODELO,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2, # Persistente
                expiration=str(MODELO_TTL) # Con TTL individual por mensaje
            )
        )
        print(f"[EXITO] Modelo publicado en cola '{QUEUE_MODELO}' (TTL: {MODELO_TTL/1000}s)")
        return True
    
    # # Verifica si el modelo sigue en la cola (no ha expirado TTL)
    # def modelo_existe_en_cola(self):
    #     try:
    #         # Intentar obtener informacion de la cola
    #         # passive=True: Solo verifica sin crear la cola
    #         method = self.channel.queue_declare(queue=QUEUE_MODELO, passive=True)
    #         # Si la cola existe y tiene mensajes, el modelo esta disponible
    #         return method.method.message_count > 0
    #     except pika.exceptions.ChannelClosedByBroker as e:
    #         # Si RabbitMQ cierra el canal (ej: cola no existe)
    #         # Recrear el canal para poder seguir operando
    #         print(f"[INFORMACION] Canal cerrado por RabbitMQ (cola expiro), recreando...")
    #         try:
    #             self.channel = self.connection.channel()
    #             # Re-declarar colas necesarias
    #             self.channel.queue_declare(queue=QUEUE_ESCENARIOS, durable=True)
    #             self.channel.queue_declare(queue=QUEUE_RESULTADOS, durable=True)
    #             self.channel.queue_declare(queue=QUEUE_COMANDOS, durable=True)
    #         except Exception as recreate_error:
    #             print(f"[ERROR] No se pudo recrear canal: {recreate_error}")
    #         return False
    #     except Exception as e:
    #         # Cualquier otro error: asumir que el modelo no esta disponible
    #         return False
    
    # Verifica si hay modelo. Solo como pasivo para obtener informacion de la cola
    # Usamos otro canal para no interferir con el principal
    def modelo_existe_en_cola_generacion(self):
        try:
            method = self.channel_generacion.queue_declare(queue=QUEUE_MODELO, passive=True)
            return method.method.message_count > 0
        except pika.exceptions.ChannelClosedByBroker:
            # Canal cerrado, recrear
            try:
                self.channel_generacion = self.connection_generacion.channel()
                # Solo declarar cola de escenarios (modelo ya existe o no importa)
                self.channel_generacion.queue_declare(queue=QUEUE_ESCENARIOS, durable=True)
            except:
                pass
            return False
        except Exception:
            return False
    
    # Genera un valor segun la distribucion especificada
    def generar_valor(self, config_variable):
        distribucion = config_variable.get('distribucion', 'uniform') # Obtener distribucion
        params = config_variable.get('parametros', {}) # Obtener parametros
        
        if distribucion == 'uniform':
            valor = random.uniform(params.get('min', 0), params.get('max', 1))
        elif distribucion == 'normal':
            valor = np.random.normal(params.get('mean', 0), params.get('std', 1))
        elif distribucion == 'exponential':
            valor = np.random.exponential(params.get('scale', 1))
        else:
            valor = random.uniform(0, 1)
        
        # Convertir a positivo si es necesario
        if valor < 0:
            valor = abs(valor)
        
        return round(valor, 4)
    
    # Generar escenario unico
    def generar_escenario_unico(self):
        max_intentos = 1000 # Maximo de intentos para generar un escenario unico
        for _ in range(max_intentos):
            escenario = {}
            for var_nombre, var_config in self.modelo_actual['variables'].items():
                escenario[var_nombre] = self.generar_valor(var_config)
            
            # Crear hash del escenario para verificar unicidad
            escenario_hash = json.dumps(escenario, sort_keys=True)
            
            if escenario_hash not in self.escenarios_generados:
                self.escenarios_generados.add(escenario_hash)
                return escenario
        
        # Si después de 1000 intentos no encuentra unico, se puede repetir
        return escenario
    
    # Funcion en hilo que genera escenarios continuamente hasta TTL o cambio de modelo
    def generacion_continua(self):
        print(f"\n[*] Iniciando generación continua de escenarios...")
        print(f"    Modelo: {self.modelo_actual['nombre']}")
        print(f"    Intervalo: {ESCENARIO_INTERVAL}s por escenario")
        print(f"    El modelo expirará automáticamente después de {MODELO_TTL/1000}s (TTL)")
        print(f"    La generación se detendrá cuando el modelo expire\n")
        
        # Crear conexion separada para este thread (evita conflictos con canal principal)
        try:
            creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
            params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds)
            self.connection_generacion = pika.BlockingConnection(params)
            self.channel_generacion = self.connection_generacion.channel()
            
            # Solo declarar cola de escenarios (la cola modelo ya existe)
            self.channel_generacion.queue_declare(queue=QUEUE_ESCENARIOS, durable=True)
        except Exception as e:
            print(f"[ERROR] No se pudo crear conexión para generación: {e}")
            return
        
        # Loop simple: genera hasta que se detenga o el modelo expire
        while self.generando:
            try:
                # Verificar cada 100 escenarios si el modelo sigue en la cola
                if self.total_generados > 0 and self.total_generados % 100 == 0:
                    if not self.modelo_existe_en_cola_generacion(): # Si ya no esta el modelo
                        print(f"\n[ADVERTENCIA] Modelo expirado (TTL cumplido)")
                        print(f"[*] RabbitMQ eliminó el modelo de la cola")
                        print(f"[*] Deteniendo generación de escenarios...")
                        self.generando = False
                        break
                
                # Generar escenario unico
                escenario = self.generar_escenario_unico()
                
                # Publicar escenario
                body = json.dumps(escenario)
                self.channel_generacion.basic_publish(
                    exchange='',
                    routing_key=QUEUE_ESCENARIOS,
                    body=body.encode('utf-8'),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                
                self.total_generados += 1
                
                # Mostrar progreso cada 100 escenarios
                if self.total_generados % 100 == 0:
                    print(f"[INFORMACION] Generados: {self.total_generados} escenarios | "
                          f"Unicos: {len(self.escenarios_generados)}")
                
                # Esperar intervalo
                time.sleep(ESCENARIO_INTERVAL)
                
            except Exception as e:
                print(f"[ERROR] Error al generar escenario: {e}")
                time.sleep(1)
        
        # Cerrar conexion del thread (no la principal)
        try:
            if self.connection_generacion and self.connection_generacion.is_open:
                self.connection_generacion.close()
        except:
            pass
        
        print(f"\n[EXITO] Generación detenida")
        print(f"    Total generados: {self.total_generados}")
        print(f"    Escenarios únicos: {len(self.escenarios_generados)}")
        print(f"\n[*] Esperando nuevos comandos...")
        print(f"\n[*] Esperando nuevos comandos...")
    
    # Cambiar modelo
    def cambiar_modelo(self, comando):
        nombre_archivo = comando.get('modelo') # Obtener modelo nuevo
        
        if not nombre_archivo:
            print("[ERROR] Comando sin especificar modelo")
            return False
        
        print(f"\n{'=' * 60}")
        print(f" CAMBIO DE MODELO")
        print(f"{'=' * 60}")
        
        # Detener generacion actual si existe
        if self.generando: # Si se estan generando escenarios
            print(f"[ADVERTENCIA] Deteniendo generacion actual...")
            self.generando = False  # Indicar que ya no se estan generando escenarios
            if self.thread_generacion:
                self.thread_generacion.join(timeout=5)
        
        # Cargar nuevo modelo
        modelo = self.cargar_modelo(nombre_archivo)
        if not modelo:
            print(f"[ERROR] No se pudo cargar {nombre_archivo}")
            return False
        
        self.modelo_actual = modelo
        
        # Reiniciar conjunto de escenarios únicos
        self.escenarios_generados.clear()
        self.total_generados = 0
        
        # Publicar modelo
        if not self.publicar_modelo():
            return False
        
        # Esperar a que workers lean el modelo
        print(f"[*] Esperando 2 segundos para que workers lean el modelo...")
        time.sleep(2)
        
        # Iniciar generacion continua en thread
        self.generando = True
        self.thread_generacion = threading.Thread(target=self.generacion_continua, daemon=True)
        self.thread_generacion.start()
        
        print(f"[EXITO] Modelo cambiado exitosamente")
        print(f"{'=' * 60}\n")
        
        return True
    
    # Escuchar comandos de la cola de comandos (del dashboard)
    def escuchar_comandos(self):
        print(f"\n[*] Escuchando comandos en cola '{QUEUE_COMANDOS}'...")
        print(f"[*] Esperando instrucciones desde Dashboard...")
        
        def callback(ch, method, props, body): # Cuando consume comando
            try:
                comando = json.loads(body.decode('utf-8'))
                tipo = comando.get('comando')
                
                print(f"\n[INFORMACION] Comando recibido: {tipo}")
                
                if tipo == 'cambiar_modelo': # Comando camiar modelo
                    self.cambiar_modelo(comando)
                elif tipo == 'detener': # Comando detener
                    print(f"[ADVERTENCIA] Comando de detención recibido")
                    self.generando = False
                else:
                    print(f"[ADVERTENCIA] Comando desconocido: {tipo}")
                
                ch.basic_ack(delivery_tag=method.delivery_tag) # Avisa que se proceso comando
                
            except Exception as e:
                print(f"[ERROR] Error procesando comando: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        self.channel.basic_qos(prefetch_count=1) # Solo puede consumir un comando
        self.channel.basic_consume(
            queue=QUEUE_COMANDOS,
            on_message_callback=callback,
            auto_ack=False
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
    
    # Cierra conexiones principal y de hilo
    def cerrar(self):
        self.generando = False
        if self.connection_generacion and self.connection_generacion.is_open:
            try:
                self.connection_generacion.close()
            except:
                pass
        if self.connection:
            try:
                self.connection.close()
            except:
                pass

def main():
    productor = ProductorServicio() # Creamos al productor
    
    # Parar al productor
    def signal_handler(sig, frame):
        print("\n\n" + "=" * 60)
        print(" Productor Servicio - Deteniendo")
        print("=" * 60)
        productor.generando = False # Ya no genera escenarios
        productor.cerrar() # Cierra conexiones
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        print("=" * 60)
        print(" PRODUCTOR SERVICIO - Simulación Montecarlo")
        print("=" * 60)
        print()
        
        # Conectar
        productor.conectar() # Conectamos a RabbitMQ y creamos colas
        
        # Verificar que existan archivos de modelos
        modelos_disponibles = []
        for archivo in ['modelo_tiempo.json', 'modelo_area.json', 'modelo_beneficio.json']:
            if Path(archivo).exists():
                modelos_disponibles.append(archivo)
        
        if modelos_disponibles:
            print(f"\n[INFORMACION] Modelos disponibles:")
            for modelo in modelos_disponibles:
                print(f"    - {modelo}")
            print()
        
        print("[INFORMACION] Esperando comandos desde Dashboard para cargar modelo...")
        print("[INFORMACION] Usa dashboard_gui.py para controlar el sistema")
        print()
        
        # Escuchar comandos
        productor.escuchar_comandos() # Escuchar comandos del dashboard
        
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        productor.cerrar()

if __name__ == "__main__":
    main()
