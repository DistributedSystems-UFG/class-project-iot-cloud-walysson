import grpc
import iot_service_pb2
import iot_service_pb2_grpc

if __name__ == '__main__':
    channel = grpc.insecure_channel('localhost:50051')
    username = input("Enter username: ")
    password = input("Enter password: ")
    stub = iot_service_pb2_grpc.IoTServiceStub(channel)
    response = stub.Login(iot_service_pb2.UserRequest(name=username, password=password))
    print("Login status: " + str(response.status))
    print("Token: " + response.token)
    # retrieve temperature
    response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(sensorName='my_sensor', token=response.token))
    print("Temperature: " + response.temperature)
