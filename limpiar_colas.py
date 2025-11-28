#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Limpia todas las colas generadas en RabbitMQ por si tenemos algun problema de compatibilidad

import pika
import sys
from config import RABBIT_HOST, RABBIT_USER, RABBIT_PASS
from config import (QUEUE_MODELO, QUEUE_ESCENARIOS, QUEUE_RESULTADOS,
                   QUEUE_COMANDOS)

def limpiar_colas():
    try:
        print("=" * 60)
        print(" LIMPIEZA DE COLAS - RabbitMQ")
        print("=" * 60)
        print()
        
        # Conectar a RabbitMQ
        creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        params = pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        print("[*] Conectado a RabbitMQ")
        print()
        
        # Lista de colas a eliminar
        colas = [QUEUE_MODELO, QUEUE_ESCENARIOS, QUEUE_RESULTADOS,
                QUEUE_COMANDOS]
        
        for cola in colas:
            try:
                channel.queue_delete(queue=cola)
                print(f"[EXITO] Cola '{cola}' eliminada")
            except Exception as e:
                if "NOT_FOUND" in str(e):
                    print(f"[INFORMACION] Cola '{cola}' no existe (ok)")
                else:
                    print(f"[ERROR] Error al eliminar cola '{cola}': {e}")
        
        connection.close()
        
        print()
        print("=" * 60)
        print(" Limpieza completada")
        print("=" * 60)
        print()
        
        return 0
        
    except Exception as e:
        print()
        print("[ERROR]", e)
        print()
        print("Verifica que RabbitMQ esté corriendo y las credenciales")
        print("sean correctas en config.py")
        return 1

if __name__ == "__main__":
    sys.exit(limpiar_colas())
