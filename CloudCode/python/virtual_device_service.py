from kafka import KafkaConsumer, KafkaProducer
from const import *
import threading

from concurrent import futures
import logging
import sqlite3
import grpc
import iot_service_pb2
import iot_service_pb2_grpc
import jwt
import datetime
import create_db

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
        if float(msg.value.decode()) >= 80:
            produce_led_command(1, 'red')
        else:
            produce_led_command(0, 'red')
        current_light_level = msg.value.decode()

def produce_led_command(state, ledname):
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER+':'+KAFKA_PORT)
    producer.send('ledcommand', key=ledname.encode(), value=str(state).encode())
    return state

def generate_token(name):
    payload = {
        'name': name,
        'exp': datetime.datetime.utcnow() + datetime.timedelta(days=7)
    }
    token = jwt.encode(payload, 'scret', algorithm='HS256')
    return token

class IoTServer(iot_service_pb2_grpc.IoTServiceServicer):

    def SayTemperature(self, request, context):
        if self.is_authenticated(request.token):
            create_db.insert_user_device_value(1, 1, current_temperature)
            return iot_service_pb2.TemperatureReply(temperature=current_temperature)
        return iot_service_pb2.TemperatureReply(temperature="")
    
    def BlinkLed(self, request, context):
        if self.is_authenticated(request.token):
            print ("Blink led ", request.ledname)
            print ("...with state ", request.state)
            produce_led_command(request.state, request.ledname)
            led_id = 3 if request.ledname == 'red' else 4
            create_db.insert_user_device_value(1, led_id, request.state)
            # Update led state of twin
            led_state[request.ledname] = request.state
            return iot_service_pb2.LedReply(ledstate=led_state)
        return iot_service_pb2.LedReply(ledstate={})

    def SayLightLevel(self, request, context):
        if self.is_authenticated(request.token):
            create_db.insert_user_device_value(1,2, current_light_level)
            return iot_service_pb2.LightLevelReply(lightLevel=current_light_level)
        return iot_service_pb2.LightLevelReply(lightLevel="")

    def Login(self, request, context):
        print ("Login ", request.name)
        print ("...with password ", request.password)
        if create_db.verify_login(request.name, request.password):
            return iot_service_pb2.UserResponse(status=True, token=generate_token(request.name))
        else:
            return iot_service_pb2.UserResponse(status=False, token="")
    
    def is_authenticated(self, token):
        try:
            jwt.decode(token, 'scret', algorithms=['HS256'])
            return True
        except:
            return False

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    iot_service_pb2_grpc.add_IoTServiceServicer_to_server(IoTServer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    create_db.run()
    trd1 = threading.Thread(target=consume_temperature)
    trd1.start()

    trd2 = threading.Thread(target=consume_light_level)
    trd2.start()

    # Initialize the state of the leds on the actual device
    for color in led_state.keys():
        produce_led_command (led_state[color], color)
    serve()
