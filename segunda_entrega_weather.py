import psycopg2
import requests
import datetime
import json
import pytz

# Función para cargar la configuración desde el archivo
def load_config(filename='config.json'):
    with open(filename) as f:
        config = json.load(f)
    return config

# Función para solicitar la ciudad al usuario
def get_city_from_user():
    city = input("Por favor, ingrese el nombre de la ciudad: ")
    return city

# Función para obtener la hora local de la ciudad desde la respuesta de la API de OpenWeatherMap
def get_local_time(city):
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,  # Ciudad proporcionada por el usuario
        "appid": "e3a017f12eb6f803c0af4e91a1603a29",
        "units": "metric"  # Unidades métricas (Celsius)
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        timestamp = data.get('dt')  # Obtener el timestamp de la respuesta
        if timestamp:
            timezone = data.get('timezone')  # Obtener la zona horaria de la ciudad
            local_time = datetime.datetime.utcfromtimestamp(timestamp)
            local_timezone = pytz.timezone(pytz.country_timezones[data['sys']['country']][0])
            local_time = local_time.replace(tzinfo=pytz.utc).astimezone(local_timezone)  # Convertir a hora local
            local_time_gmt = local_time.replace(tzinfo=None)  # Eliminar la información de la zona horaria para convertir a GMT
            return data, local_time_gmt  # Devolver también los datos de la respuesta y la hora local en GMT
    return None, None

# Función para conectar a Redshift
def connect_to_redshift():
    try:
        config = load_config()
        url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
        data_base = "data-engineer-database"
        user = "santy_vidal_coderhouse"
        pwd = config['password']
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conectado a Redshift con éxito!")

        # Crear la tabla si no existe
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
                            city TEXT,
                            temperature REAL,
                            humidity REAL,
                            pressure REAL,
                            wind_speed REAL,
                            ingestion_timestamp TIMESTAMP,
                            local_time TIMESTAMP 
                        )''')
        conn.commit()
        cursor.close()
        return conn
    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)
        return None

# Función para insertar datos en la tabla "weather"
def insert_weather_data(conn, city, data, local_time):
    try:
        cursor = conn.cursor()
        cursor.execute('''INSERT INTO weather (city, temperature, humidity, pressure, wind_speed, ingestion_timestamp, local_time)
                          VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                       (city, data['main']['temp'], data['main']['humidity'], data['main']['pressure'],
                        data['wind']['speed'], datetime.datetime.now(), local_time))
        conn.commit()
        print("Datos almacenados correctamente en Redshift.")
    except Exception as e:
        print("Error al insertar datos en Redshift:")
        print(e)

# Función para borrar filas duplicadas en la tabla "weather"
def delete_duplicate_rows(conn):
    try:
        cursor = conn.cursor()
        cursor.execute('''DELETE FROM weather
                          WHERE (city, ingestion_timestamp) NOT IN
                                (SELECT city, MAX(ingestion_timestamp)
                                 FROM weather
                                 GROUP BY city)''')
        conn.commit()
        print("Los duplicados fueron borrados.")
    except Exception as e:
        print("Error al eliminar filas duplicadas:")
        print(e)

# Conectar a Redshift
conn = connect_to_redshift()

if conn:
    # Obtener la ciudad desde el usuario
    city = get_city_from_user()

    # Obtener la hora local de la ciudad
    data, local_time = get_local_time(city)
    # Verificar que los datos y la hora local estén disponibles
    if data and local_time:  
        # Insertar datos en la tabla "weather"
        insert_weather_data(conn, city, data, local_time)

        # Borrar filas duplicadas en la tabla "weather"
        delete_duplicate_rows(conn)

        conn.close()
    else:
        print("No se pudo obtener la hora local de la ciudad.")
