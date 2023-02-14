from django.http import JsonResponse
import grpc
import iot_service_pb2
import iot_service_pb2_grpc
from django.shortcuts import render 
from django.shortcuts import redirect

def login_view(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']

        # Open a gRPC channel to the remote service
        with grpc.insecure_channel('localhost:50051') as channel:
            # Create a gRPC stub for the IoTService service
            stub = iot_service_pb2_grpc.IoTServiceStub(channel)

            # Call the Login method
            response = stub.Login(iot_service_pb2.UserRequest(name=username, password=password))

            if response.status:
                # Save the token to the session so it can be used in future requests
                request.session['token'] = response.token
                return redirect('home') # Replace 'home' with the name of your desired redirect URL
            else:
                # Return an error message to the user
                return render(request, 'login.html', {'error': 'Invalid username or password'})
    elif request.method == 'GET':
        return render(request, 'usermanagement/login.html')


def home(request):
    return render(request, 'usermanagement/home.html')


def temperature(request):
    token = request.headers['Authorization']
    channel = grpc.insecure_channel('localhost:50051')
    stub = iot_service_pb2_grpc.IoTServiceStub(channel)
    response = stub.SayTemperature(iot_service_pb2.TemperatureRequest(sensorName='my_sensor', token=token))
    return JsonResponse({'temperature': response.temperature})