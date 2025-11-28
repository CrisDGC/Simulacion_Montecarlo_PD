#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Dashboard

import tkinter as tk
from tkinter import ttk, messagebox
import pika
import json
import time
import threading
from pathlib import Path
from config import (RABBIT_HOST, RABBIT_USER, RABBIT_PASS,
                   QUEUE_COMANDOS, QUEUE_RESULTADOS, MODELO_TTL)

class DashboardGUI:
    def __init__(self, root):
        # Variables para la interfaz
        self.root = root
        self.root.title("Dashboard - Simulación Montecarlo Distribuida")
        self.root.geometry("900x650")
        self.root.resizable(True, True)
        
        # Variables para la logica
        self.connection = None # Conexion
        self.channel = None # Canal
        self.modelo_actual = tk.StringVar(value="Sin modelo cargado") # Modelo que se quiere cargar
        self.total_resultados = 0 # Total resultados
        self.resultados = [] # Resultados obtenidos
        self.workers_stats = {} # Estadisticas de los workers
        self.tiempo_inicio = None # Tiempo inicial del modelo
        self.escuchando = False # Escuchar resultados de RESULTADOS
        self.ultimo_resultado_tiempo = None # 
        
        # Modelos disponibles
        self.modelos_disponibles = self.detectar_modelos()
        
        # Crear interfaz
        self.crear_interfaz()
        
        # Conectar a RabbitMQ
        self.conectar()
        
        # Iniciar escucha de resultados
        self.iniciar_escucha()
        
        # Actualizar UI cada cierto tiempo
        self.actualizar_ui()
    
    # Detectar modelos disponibles
    def detectar_modelos(self):
        modelos = []
        # Directamente pasamos la lista de los nombres con los modelos
        # para que no dependa de tener los modelos en archivos
        for archivo in ['modelo_tiempo.json', 'modelo_area.json', 'modelo_beneficio.json']:
            modelos.append({
                        'archivo': archivo,
                    })
        return modelos
    
    # Crea interfaz grafica
    def crear_interfaz(self):
        # FRAME SUPERIOR: Control de Modelos
        frame_control = tk.LabelFrame(self.root, text="Control de Modelos", font=('Arial', 12, 'bold'))
        frame_control.pack(fill=tk.X, padx=10, pady=10)
        
        # Modelo actual
        tk.Label(frame_control, text="Modelo Actual:", font=('Arial', 10, 'bold')).grid(
            row=0, column=0, padx=10, pady=5, sticky=tk.W
        )
        lbl_modelo = tk.Label(frame_control, textvariable=self.modelo_actual, font=('Arial', 10),
                fg='darkgreen', wraplength=400)
        lbl_modelo.grid(row=0, column=1, columnspan=2, padx=10, pady=5, sticky=tk.W)
        
        # Selector de modelos
        tk.Label(frame_control, text="Seleccionar Modelo:", font=('Arial', 10)).grid(
            row=1, column=0, padx=10, pady=5, sticky=tk.W
        )
        
        self.combo_modelos = ttk.Combobox(frame_control, width=40, state='readonly')
        # Nombres de los modelos en la combobox
        self.combo_modelos['values'] = [
            f"{m['archivo']}" for m in self.modelos_disponibles
        ]
        if self.modelos_disponibles:
            self.combo_modelos.current(0)
        self.combo_modelos.grid(row=1, column=1, padx=10, pady=5, sticky=tk.W)
        
        # Boton Insertar modelo
        btn_cambiar = tk.Button(
            frame_control, 
            text="Insertar Modelo", 
            command=self.cambiar_modelo,
            bg='#4CAF50',
            fg='white',
            font=('Arial', 10, 'bold'),
            padx=20,
            pady=5
        )
        btn_cambiar.grid(row=1, column=2, padx=10, pady=5)
        
        # TTL info
        ttl_segundos = MODELO_TTL / 1000
        tk.Label(frame_control, 
                text=f"TTL del modelo: {int(ttl_segundos/60)} min ({ttl_segundos}s)", 
                font=('Arial', 9),
                fg='gray').grid(row=2, column=1, padx=10, pady=5, sticky=tk.W)
        
        # FRAME MEDIO: Estadisticas Globales
        frame_stats = tk.LabelFrame(self.root, text="Estadísticas Globales", 
                                    font=('Arial', 12, 'bold'))
        frame_stats.pack(fill=tk.X, padx=10, pady=10)
        
        # Grid de estadisticas
        self.lbl_total = tk.Label(frame_stats, text="Total Resultados: 0", 
                                 font=('Arial', 10))
        self.lbl_total.grid(row=0, column=0, padx=20, pady=5)
        
        self.lbl_media = tk.Label(frame_stats, text="Media: -", font=('Arial', 10))
        self.lbl_media.grid(row=0, column=1, padx=20, pady=5)
        
        self.lbl_desv = tk.Label(frame_stats, text="Desv: -", font=('Arial', 10))
        self.lbl_desv.grid(row=1, column=0, padx=20, pady=5)
        
        self.lbl_min = tk.Label(frame_stats, text="Min: -", font=('Arial', 10))
        self.lbl_min.grid(row=1, column=1, padx=20, pady=5)
        
        self.lbl_max = tk.Label(frame_stats, text="Max: -", font=('Arial', 10))
        self.lbl_max.grid(row=1, column=2, padx=20, pady=5)
        
        self.lbl_tiempo = tk.Label(frame_stats, text="Tiempo: 00:00", 
                                   font=('Arial', 10))
        self.lbl_tiempo.grid(row=0, column=2, padx=20, pady=5)
        
        # FRAME INFERIOR: Workers
        frame_workers = tk.LabelFrame(self.root, text="Estadísticas por Worker", 
                                     font=('Arial', 12, 'bold'))
        frame_workers.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Tabla de workers
        self.tree_workers = ttk.Treeview(
            frame_workers,
            columns=('worker', 'procesados', 'porcentaje', 'ultimo'),
            show='headings',
            height=6  # 6 workers visibles
        )
        
        self.tree_workers.heading('worker', text='Worker ID')
        self.tree_workers.heading('procesados', text='Procesados')
        self.tree_workers.heading('porcentaje', text='Porcentaje')
        self.tree_workers.heading('ultimo', text='Último Resultado')
        
        self.tree_workers.column('worker', width=100, anchor=tk.CENTER)
        self.tree_workers.column('procesados', width=150, anchor=tk.CENTER)
        self.tree_workers.column('porcentaje', width=400, anchor=tk.W)
        self.tree_workers.column('ultimo', width=150, anchor=tk.CENTER)
        
        self.tree_workers.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(frame_workers, orient=tk.VERTICAL, 
                                 command=self.tree_workers.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree_workers.configure(yscrollcommand=scrollbar.set)
        
        # FRAME INFERIOR: Log
        frame_log = tk.LabelFrame(self.root, text="Log de Eventos", 
                                 font=('Arial', 12, 'bold'))
        frame_log.pack(fill=tk.X, padx=10, pady=10)  # Solo fill X, no expand
        
        # Frame contenedor para Text y Scrollbar
        log_container = tk.Frame(frame_log)
        log_container.pack(fill=tk.BOTH, padx=5, pady=5)
        
        # Text widget con altura fija
        self.text_log = tk.Text(log_container, height=8, font=('Courier', 9), wrap=tk.WORD)
        self.text_log.pack(side=tk.LEFT, fill=tk.BOTH)
        
        # Scrollbar vertical
        scrollbar_log = ttk.Scrollbar(log_container, orient=tk.VERTICAL, 
                                     command=self.text_log.yview)
        scrollbar_log.pack(side=tk.RIGHT, fill=tk.Y)
        self.text_log.configure(yscrollcommand=scrollbar_log.set)
        
        # Mensaje inicial
        self.agregar_log("Dashboard iniciado")
        self.agregar_log("Esperando resultados o cambia el modelo manualmente...")
    
    # Conectar con RabbitMQ
    def conectar(self):
        try:
            creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
            params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            self.channel.queue_declare(queue=QUEUE_COMANDOS, durable=True) # Enviamos comandos del dashboard
            self.channel.queue_declare(queue=QUEUE_RESULTADOS, durable=True) # Consumimos resultados
            
            self.agregar_log("[EXITO] Conectado a RabbitMQ")
        except Exception as e:
            self.agregar_log(f"[ERROR] Error de conexión: {e}")
            messagebox.showerror("Error de Conexión", 
                               f"No se pudo conectar a RabbitMQ:\n{e}")
    
    # Envia comando a la cola de comandos para cambiar de modelo
    def cambiar_modelo(self):
        # Si no hay modelos disponibles
        if not self.modelos_disponibles:
            messagebox.showwarning("Sin Modelos", 
                                 "No hay modelos disponibles")
            return
        
        # Se selecciona uno de los modelos
        seleccion = self.combo_modelos.current()
        if seleccion < 0:
            messagebox.showwarning("Selección", 
                                 "Selecciona un modelo primero")
            return
        
        modelo_info = self.modelos_disponibles[seleccion] # Del modelo que elegimos
        archivo = modelo_info['archivo'] # Extraemos su nombre de archivo
        #nombre = modelo_info['nombre']
        
        # Confirmar
        respuesta = messagebox.askyesno(
            "Cambiar Modelo",
            f"¿Cambiar al modelo:\n\n({archivo})?\n\n"
            f"Esto detendrá la generación actual y purgará escenarios pendientes."
        )
        
        if not respuesta:
            return
        
        # Enviar comando
        comando = {
            'comando': 'cambiar_modelo',
            'modelo': archivo,
            'timestamp': time.time()
        }
        # Se publica en la cola de comandos
        try:
            body = json.dumps(comando)
            self.channel.basic_publish(
                exchange='',
                routing_key=QUEUE_COMANDOS,
                body=body.encode('utf-8'),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            
            self.agregar_log(f"[INFORMACION] Comando enviado: Cambiar a '{archivo}'")
            self.agregar_log(f"  Esperando confirmación desde productor...")
            
            # Actualizar UI inmediatamente
            self.modelo_actual.set(f"{archivo} (cargando...)")
            
            # Reiniciar estadísticas
            self.total_resultados = 0
            self.resultados = []
            self.workers_stats = {}
            self.tiempo_inicio = None
            
        except Exception as e:
            self.agregar_log(f"[ERROR] Error al enviar comando: {e}")
            messagebox.showerror("Error", f"Error al enviar comando:\n{e}")
    
    # Iniciamos hilo para escuchar resultados
    def iniciar_escucha(self):
        self.escuchando = True
        thread_resultados = threading.Thread(target=self.escuchar_resultados, daemon=True)
        thread_resultados.start()
    
    # Escucha cola de resultados
    def escuchar_resultados(self):
        try:
            # Crear conexión separada para este hilo
            creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
            params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            
            channel.queue_declare(queue=QUEUE_RESULTADOS, durable=True) # RESULTADOS vamos a escuchar
            channel.basic_qos(prefetch_count=50) # Si podemos consumir varios
            
            def callback(ch, method, props, body):
                try:
                    data = json.loads(body.decode('utf-8')) # Cargamos resultado
                    self.procesar_resultado(data) # Procesamos resultado
                    ch.basic_ack(delivery_tag=method.delivery_tag) # Avisamos que ya lo procesamos
                except Exception as e:
                    print(f"Error procesando resultado: {e}")
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            channel.basic_consume(
                queue=QUEUE_RESULTADOS,
                on_message_callback=callback,
                auto_ack=False
            )
            
            channel.start_consuming()
            
        except Exception as e:
            print(f"Error en escucha de resultados: {e}")
    
    # Procesa un resultado recibido
    def procesar_resultado(self, data):
        # DETECTAR CAMBIO DE MODELO y reiniciar estadísticas
        modelo_nombre = data.get('modelo', 'Desconocido') # Vemos el modelo del resultado recibido
        
        # Si es diferente el modelo seleccionado que el modelo del resultado de la cola
        if self.modelo_actual.get() != modelo_nombre and modelo_nombre != 'Desconocido':
            # Cambio de modelo detectado
            print(f"\n[ADVERTENCIA] Cambio de modelo detectado: {self.modelo_actual.get()} -> {modelo_nombre}")
            self.modelo_actual.set(modelo_nombre) # Actualizamos el modelo actual
            
            # REINICIAR todas las estadisticas
            self.total_resultados = 0
            self.resultados = []
            self.workers_stats = {}
            self.tiempo_inicio = None
            self.ultimo_resultado_tiempo = None
            
            self.agregar_log(f"[EXITO] Modelo cambiado: {modelo_nombre}")
            self.agregar_log(f"  Estadísticas reiniciadas")
            print(f"[EXITO] Estadísticas reiniciadas para nuevo modelo\n")
        
        # Procesar resultado normalmente
        self.total_resultados += 1
        self.resultados.append(data['resultado']) # Agregamos el resultado a la lista de resultados (local)
        self.ultimo_resultado_tiempo = time.time() # Registramos tiempo del ultimo resultado
        
        # Actualizar stats por worker
        worker_id = data.get('worker_id', 'desconocido') # Obtener id del worker
        if worker_id not in self.workers_stats: # Si es nuevo, se registra en el diccionario de workers
            self.workers_stats[worker_id] = {
                'procesados': 0,
                'ultimo_resultado': 0
            }
        
        self.workers_stats[worker_id]['procesados'] += 1 # Aumentamos contador de resultado procesado de cierto worker
        self.workers_stats[worker_id]['ultimo_resultado'] = data['resultado'] # Guardamos ultimo resultado
        
        # Iniciar tiempo si es el primer resultado
        if self.tiempo_inicio is None:
            self.tiempo_inicio = time.time()
    
    # Actualiza la interfaz cada cierto tiempo
    def actualizar_ui(self):
        # Actualizar estadisticas globales
        if self.resultados:
            import numpy as np
            media = np.mean(self.resultados) # Media de los resultados
            desv = np.std(self.resultados) # Desviacion de los resultados
            minimo = np.min(self.resultados) # Minimo de los resultados
            maximo = np.max(self.resultados) # Maximo de los resultados
            # Lo colocamos en la interfaz
            self.lbl_media.config(text=f"Media: {media:.4f}")
            self.lbl_desv.config(text=f"Desv: {desv:.4f}")
            self.lbl_min.config(text=f"Min: {minimo:.4f}")
            self.lbl_max.config(text=f"Max: {maximo:.4f}")
        
        self.lbl_total.config(text=f"Total Resultados: {self.total_resultados}") # Se muestra el total de resultados
        
        # Actualizar tiempo
        if self.tiempo_inicio:
            transcurrido = int(time.time() - self.tiempo_inicio)
            minutos = transcurrido // 60
            segundos = transcurrido % 60
            self.lbl_tiempo.config(text=f"Tiempo: {minutos:02d}:{segundos:02d}")
        
        # Detectar si modelo expiro (no hay resultados en 10 segundos)
        if self.ultimo_resultado_tiempo:
            tiempo_sin_resultados = time.time() - self.ultimo_resultado_tiempo
            if tiempo_sin_resultados > 10 and self.modelo_actual.get() != "Sin modelo cargado":
                # Posible expiracion
                self.agregar_log(" No se reciben resultados (modelo expirado)")
                self.ultimo_resultado_tiempo = None
        
        # Actualizar tabla de workers (por si se sale alguno)
        self.tree_workers.delete(*self.tree_workers.get_children())
        
        # Mostrar estadisticas de los workers
        for worker_id in sorted(self.workers_stats.keys()):
            stats = self.workers_stats[worker_id]
            procesados = stats['procesados']
            # Porcentaje de procesados del worker respecto al total
            porcentaje = (procesados / self.total_resultados * 100) if self.total_resultados > 0 else 0
            ultimo = stats['ultimo_resultado']
            
            # Barra de progreso visual
            barra_longitud = int(porcentaje / 2)  # 50 caracteres máximo
            barra = "█" * barra_longitud + "░" * (50 - barra_longitud)
            texto_porcentaje = f"{porcentaje:5.1f}% {barra}"
            
            # Insertamos al worker visualmente
            self.tree_workers.insert('', tk.END, values=(
                f"Worker {worker_id}",
                procesados,
                texto_porcentaje,
                f"{ultimo:.4f}"
            ))
        
        # Programar siguiente actualizacion (cada segundo se actualiza)
        self.root.after(1000, self.actualizar_ui)
    
    # Agregar log
    def agregar_log(self, mensaje):
        timestamp = time.strftime("%H:%M:%S") # Hora
        self.text_log.insert(tk.END, f"[{timestamp}] {mensaje}\n") # En la parte del log
        
        # Autoscroll: mover al final
        self.text_log.see(tk.END)
        
        # Limitar a últimas 200 líneas para no saturar
        lineas = int(self.text_log.index('end-1c').split('.')[0])
        if lineas > 200:
            self.text_log.delete('1.0', f'{lineas-200}.0')
    
    # Cerrar conexion
    def cerrar(self):
        self.escuchando = False
        if self.connection:
            try:
                self.connection.close()
            except:
                pass

def main():
    # Para interfaz
    root = tk.Tk()
    app = DashboardGUI(root) # Creamos interfaz
    
    def on_closing():
        if messagebox.askokcancel("Salir", "¿Deseas cerrar el Dashboard?"):
            app.cerrar() # Cerrar conexion si se cierra el programa
            root.destroy()
    
    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()
