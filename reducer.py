import socket
import threading
from utils import get_kv_rpc_client, deserialize_fn

class ReducerThread(threading.Thread):

    def __init__(self, reducer_id, MASTER_IP, MASTER_PORT, KV_STORE_IP, KV_STORE_PORT):
        
        threading.Thread.__init__(self)

        self.reducer_id = reducer_id
        self.MASTER_IP = MASTER_IP
        self.MASTER_PORT = MASTER_PORT
        self.KV_STORE_IP = KV_STORE_IP
        self.KV_STORE_PORT = KV_STORE_PORT
        self.kv_client = None

    def run(self):

        # Creating reducer socket client
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((self.MASTER_IP, self.MASTER_PORT))

        # Connecting to KV Store RPC client
        self.kv_client = get_kv_rpc_client(self.KV_STORE_IP, self.KV_STORE_PORT)
        if not self.kv_client:
            msg = f"{self.reducer_id}:Unable to connect to KV Store!:-1"
            client_socket.send(bytes(msg.encode()))
        
        msg = f"{self.reducer_id}:Connected to KV Store, Waiting for task..:100"
        client_socket.send(bytes(msg.encode()))

        # Starting communication with Master
        while True:
            """
                Messages From Master:
                1) terminate
                2) task:cache_key:<reducer_fn_length>
                3) <reducer_fn>

                Messages To Master:
                1) reducer-id:<message>:<status_code>
            """
            # msg format - task:application:key1,key2,key3
            msg = client_socket.recv(256)
            msg = msg.decode()

            if msg == "terminate":
                print(f"Terminating.. {self.reducer_id}")
                break

            msg = msg.split(":")
            if msg[0] == "task":

                # Requesting cache_keys from Master
                cache_keys_length = int(msg[1])
                cache_keys = client_socket.recv(cache_keys_length)
                cache_keys = cache_keys.decode()
                cache_keys = cache_keys.split(",")

                # Requesting reducer function from Master
                reducer_fn_length = int(msg[2])
                reducer_fn_str = client_socket.recv(reducer_fn_length)
                reducer_fn = deserialize_fn(reducer_fn_str)

                # Initializing Reducing Task
                status_code = reducer_fn(self.reducer_id, self.kv_client, cache_keys)

                message = ""
                if status_code == 200:
                    message = f"{self.reducer_id}:Reducing Task Successfully Completed:{status_code}"
                elif status_code == 300:
                    message = f"{self.reducer_id}:Key Not Found in KV Store:{status_code}"
                elif status_code == 108:
                    message = f"{self.reducer_id}:Failed Reducer Task:{status_code}"
                else:
                    message = f"{self.reducer_id}:Failed with, Unknown Error:{status_code}"
                client_socket.send(message.encode())