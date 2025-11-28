# Configuración de RabbitMQ
RABBIT_HOST = "172.31.12.207"
RABBIT_USER = "admin"
RABBIT_PASS = "admin"

# Colas del sistema
QUEUE_MODELO = "modelo" # Se publica el modelo para ser consumido por workers
QUEUE_ESCENARIOS = "escenarios" # Se publican varios escenarios de acuerdo al modelo
QUEUE_RESULTADOS = "resultados" # Los workers publican sus resultados. Dashboard los consume para mostrar estadisticas
QUEUE_COMANDOS = "comandos"  # Comandos de la interfaz (dashboard) para que el productor cargue modelos diferentes

# TTL del modelo en milisegundos
MODELO_TTL = 60000  # 2 minutos

# Intervalo de generacion de escenarios (segundos)
ESCENARIO_INTERVAL = 0.05
