# Sistema de Simulación Montecarlo Distribuida

Sistema distribuido para simulaciones Montecarlo utilizando RabbitMQ como middleware.

## Características

- Generación continua de escenarios únicos
- Procesamiento distribuido con múltiples workers
- Cambio de modelos sin detener sistema
- Interfaz gráfica con estadísticas en tiempo real
- Thread-safe y escalable

## Requisitos

- Python 3.7+
- RabbitMQ Server
- Dependencias: `pika`, `numpy`, `tkinter`

## Arquitectura
<img width="1973" height="1361" alt="Diagrama_Montf" src="https://github.com/user-attachments/assets/648fb943-32d3-4c53-a885-fc3350a5e5b7" />
```

## Instalación
```bash
# Instalar dependencias
pip install pika numpy

# Instalar RabbitMQ (Depende de sistema operativo)
```

## Uso
```bash
# 1. Limpiar colas (primera vez)
python limpiar_colas.py

# 2. Iniciar productor
python productor.py

# 3. Iniciar workers (en diferentes terminales)
python worker.py 1
python worker.py 2
python worker.py 3

# 4. Iniciar dashboard
python dashboard_gui.py
```

## 👥 Autores

- Aguilar Serrano Diego Fernando
- Guevara Cano Cristhian Daniel
