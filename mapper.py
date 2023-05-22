import socket
import threading
from utils import get_kv_rpc_client, deserialize_fn

class MapperThread(threading.Thread):

    def __init__(self, mapper_id, MASTER_IP, MASTER_PORT, KV_STORE_IP, KV_STORE_PORT):
        
        threading.Thread.__init__(self)

        self.mapper_id = mapper_id
        self.MASTER_IP = MASTER_IP
        self.MASTER_PORT = MASTER_PORT
        self.KV_STORE_IP = KV_STORE_IP
        self.KV_STORE_PORT = KV_STORE_PORT
        self.kv_client = None

    def run(self):

        # Creating mapper socket client
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.MASTER_IP, self.MASTER_PORT))

        # Connecting to KV Store RPC client
        self.kv_client = get_kv_rpc_client(self.KV_STORE_IP, self.KV_STORE_PORT)
        if not self.kv_client:
            msg = f"{self.mapper_id}:Unable to connect to KV Store!:-1"
            client_socket.send(bytes(msg.encode()))
        
        msg = f"{self.mapper_id}:Connected to KV Store, Waiting for task..:100"
        client_socket.send(bytes(msg.encode()))

        # Starting communication with Master
        while True:
            """
                Messages From Master:
                1) terminate
                2) task:cache_key:<mapper_fn_length>
                3) <mapper_fn>

                Messages To Master:
                1) mapper-id:<message>:<status_code>
            """
            # msg format - task:application:key1,key2,key3
            msg = client_socket.recv(256)
            msg = msg.decode()
            if msg == "terminate":
                print(f"Terminating.. {self.mapper_id}")
                break

            msg = msg.split(":")
            if msg[0] == "task":
                cache_key = msg[1]
                mapper_fn_length = int(msg[2])
                # Requesting Mapper function from Master
                mapper_fn_str = client_socket.recv(mapper_fn_length)
                mapper_fn = deserialize_fn(mapper_fn_str)
                # Initializing Mapping Task
                response = mapper_fn(self.mapper_id, self.kv_client, cache_key)
                response = response.split(":")
                status_code = int(response[0])

                message = ""
                if status_code == 200:
                    message = f"{self.mapper_id}:Successfully Completed, Processed {int(response[1])} tokens:{status_code}"
                elif status_code == 300:
                    message = f"{self.mapper_id}:Key Not Found in KV Store:{status_code}"
                elif status_code == 108:
                    message = f"{self.mapper_id}:Failed Mapper Task:{status_code}"
                else:
                    message = f"{self.mapper_id}:Failed with, Unknown Error:{status_code}"
                client_socket.send(message.encode())

