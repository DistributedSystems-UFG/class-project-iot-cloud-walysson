from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading

from concurrent import futures
import logging
import sqlite3
import grpc
import iot_service_pb2
import iot_service_pb2_grpc

# Twin state
current_temperature = 'void'
current_light_level = 'void'
led_state = {'red':0, 'green':0}

# Kafka consumer to run on a separate thread
def consume_temperature():
    global current_temperature
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('temperature'))
    for msg in consumer:
        print ('Received Temperature: ', msg.value.decode())
        current_temperature = msg.value.decode()

# Kafka consumer to run on a separate thread
def consume_light_level():
    global current_light_level
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    consumer.subscribe(topics=('lightlevel'))
    for msg in consumer:
        print ('Received Light Level: ', msg.value.decode())
        current_light_level = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state


def verify_login(name, password):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE name=? AND password=?", (name, password))
    if c.fetchone():
        return True
    else:
        return False


def create_column_user():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS users (name TEXT, password TEXT)")
    conn.commit()
    conn.close()

def add_admin_user(name, password):
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("INSERT INTO users VALUES (?,?)", (name, password))
    conn.commit()
    conn.close()

def create_column_devices():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS devices (identifier TEXT, name TEXT, type TEXT, state TEXT)")
    conn.commit()
    conn.close()

def clear_devices_table():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("DELETE FROM devices")
    conn.commit()
    conn.close()

def create_initial_devices():
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute("INSERT INTO devices VALUES ('1', 'Temperature Sensor', 'temperature', 'void')")
    c.execute("INSERT INTO devices VALUES ('2', 'Light Sensor', 'light', 'void')")
    c.execute("INSERT INTO devices VALUES ('3', 'Red LED', 'led', '0')")
    c.execute("INSERT INTO devices VALUES ('4', 'Green LED', 'led', '0')")
    conn.commit()
    conn.close()


class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        return iot_service_pb2.TemperatureReply(temperature=current_temperature)
    
    def BlinkLed(self, request, context):
        print ("Blink led ", request.ledname)
        print ("...with state ", request.state)
        produce_led_command(request.state, request.ledname)
        # Update led state of twin
        led_state[request.ledname] = request.state
        return iot_service_pb2.LedReply(ledstate=led_state)

    def SayLightLevel(self, request, context):
        return iot_service_pb2.LightLevelReply(lightLevel=current_light_level)

    def Login(self, request, context):
        print ("Login ", request.name)
        print ("...with password ", request.password)
        if verify_login(request.name, request.password):
            return iot_service_pb2.UserResponse(status=True, token="1234567890")
        else:
            return iot_service_pb2.UserResponse(status=False, token="")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    create_column_user()
    if not verify_login('admin', 'admin'):
        add_admin_user('admin', 'admin')
    create_column_devices()
    clear_devices_table()
    create_initial_devices()
    trd1 = threading.Thread(target=consume_temperature)
    trd1.start()

    trd2 = threading.Thread(target=consume_light_level)
    trd2.start()

    # Initialize the state of the leds on the actual device
    for color in led_state.keys():
        produce_led_command (led_state[color], color)
    serve()
