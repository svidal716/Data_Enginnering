import psycopg2
import requests
import datetime
import json

# Función para cargar la configuración desde el archivo
def load_config(filename='config.json'):
    with open(filename) as f:
        config = json.load(f)
    return config

# Función para solicitar la ciudad al usuario
def get_city_from_user():
    city = input("Por favor, ingrese el nombre de la ciudad: ")
    return city


# Configuración de la conexión a Redshift
config = load_config()
url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base = "data-engineer-database"
user = "santy_vidal_coderhouse"
pwd = config['password']

try:
    conn = psycopg2.connect(
        host=url,
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("Conectado a Redshift con éxito!")

except Exception as e:
    print("No es posible conectar a Redshift")
    print(e)

# Obtener la ciudad desde el usuario
city = get_city_from_user()

# Definir la URL de la API y los parámetros necesarios
url = "http://api.openweathermap.org/data/2.5/weather"
params = {
    "q": city,  # Ciudad proporcionada por el usuario
    "appid": "e3a017f12eb6f803c0af4e91a1603a29",
    "units": "metric"  # Unidades métricas (Celsius)
}

# Hacer la solicitud GET a la API de OpenWeatherMap
response = requests.get(url, params=params)

# Verificar si la solicitud fue exitosa (código de estado 200)
if response.status_code == 200:
    # Convertir la respuesta JSON en un diccionario Python
    data = response.json()

    # Agregar columna temporal para el control de ingestión de datos
    data['ingestion_timestamp'] = datetime.datetime.now()

    # Crear un cursor para ejecutar comandos SQL
    cursor = conn.cursor()

    # Crear la tabla si no existe
    cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
                        city TEXT,
                        temperature REAL,
                        humidity REAL,
                        pressure REAL,
                        wind_speed REAL,
                        ingestion_timestamp TIMESTAMP -- Temporary column
                    )''')

    # Insertar los datos en la tabla
    cursor.execute('''INSERT INTO weather (city, temperature, humidity, pressure, wind_speed, ingestion_timestamp)
                      VALUES (%s, %s, %s, %s, %s, %s)''',
                   (data['name'], data['main']['temp'], data['main']['humidity'], data['main']['pressure'],
                    data['wind']['speed'], data['ingestion_timestamp']))

    # Commit para guardar los cambios
    conn.commit()
    conn.close()

    print("Datos almacenados correctamente en Redshift.")
else:
    print("Error al hacer la solicitud:", response.status_code)