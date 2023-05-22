import json
import time
import sys
import socket
import threading
from importlib import import_module
from utils import serialize_fn, clean_data, split_text, split_list, get_kv_rpc_client
from mapper import MapperThread
from reducer import ReducerThread

class MasterThread(threading.Thread):

    def __init__(self, client_socket, mapper_fn=None, reducer_fn=None):
        
        threading.Thread.__init__(self)
        self.client_socket = client_socket
        self.mapper_fn = mapper_fn
        self.reducer_fn = reducer_fn


    def run(self):
        """
            Message format from Mapper/Reducer: 
                <mapper_id>:<message>:<status_code>
                <reducer_id>:<message>:<status_code>
            status_code:
                100 - Requesting Task
                108 - Task Failed at Runtime
                200 - Task Completed Successfully
                300 - Cache Key Not Found in KV Store
        """
        global state
        global chunk_keys
        global reducer_keys
        
        while True:
            msg = self.client_socket.recv(1024)
            msg = msg.decode()

            msg = msg.split(":")

            client_id = msg[0]
            if "mapper" in client_id:
                
                mapper_id = client_id
                message = msg[1]
                status_code = int(msg[2])
                if status_code == 100:

                    # Assigning Task
                    if not chunk_keys:
                        print("No Data To Process..")
                        self.client_socket.send("terminate".encode())
                        sys.exit()

                    chunk = chunk_keys.pop()
                    serialized_mapper = serialize_fn(self.mapper_fn)
                    cli_msg = f"task:{chunk}:{len(serialized_mapper)}"
                    self.client_socket.send(cli_msg.encode())
                    time.sleep(0.5)
                    self.client_socket.send(serialized_mapper)

                elif status_code == 108:
                    print(f"{mapper_id}: {message}")
                
                elif status_code == 200:
                    # Task Completed
                    print(f"{mapper_id}: {message}")
                    state["mapper_ack"] += 1
                    # Terminating Mapper
                    self.client_socket.send("terminate".encode())
                    time.sleep(1)
                    sys.exit()
                else:
                    pass
            
            if "reducer" in client_id:

                reducer_id = client_id
                message = msg[1]
                status_code = int(msg[2])
                if status_code == 100:

                    # Assigning Task
                    if not reducer_keys:
                        print("No Data To Process..")
                        self.client_socket.send("terminate".encode())
                        sys.exit()

                    chunk = reducer_keys.pop()
                    chunk = ",".join(chunk)

                    serialized_reducer = serialize_fn(self.reducer_fn)
                    cli_msg = f"task:{len(chunk)}:{len(serialized_reducer)}"
                    self.client_socket.send(cli_msg.encode())
                    time.sleep(0.5)
                    # Sending Cache Keys
                    self.client_socket.send(chunk.encode())
                    time.sleep(0.5)
                    # Sending Reducer Function
                    self.client_socket.send(serialized_reducer)

                elif status_code == 108:
                    print(f"{reducer_id}: {message}")
                
                elif status_code == 200:
                    # Task Completed
                    print(f"{reducer_id}: {message}")
                    state["reducer_ack"] += 1
                    # Terminating Mapper
                    self.client_socket.send("terminate".encode())
                    time.sleep(1)
                    sys.exit()
                else:
                    pass


if __name__ == "__main__":

    global state
    global chunk_keys
    global reducer_keys

    state = {
        "mapper_sockets":[],
        "reducer_sockets":[],
        "mapper_ack":0,
        "reducer_ack":0    
    }

    # Loading config from file
    print("Loading Configuration from config.json..")
    config = json.load(open("config.json","r"))

    KV_STORE_IP = config["kv_store"]["IP"]
    KV_STORE_PORT = config["kv_store"]["PORT"]
    MASTER_IP = config["master"]["IP"]
    MASTER_PORT = config["master"]["PORT"]
    mapper_count = config["mappers"]
    reducer_count = config["reducers"]
    application_task = config["application"]
    
    # Input python file containing Mapper and Reducer Functions
    print("Importing map and reduce functions..")
    task_file = config["map_reduce_fn"]
    task_file = task_file.replace(".py","").replace("/",".")
    # Loading module from given file path
    task_module = import_module(task_file)
    # Loading mapper and reducer functions
    mapper_fn = task_module.mapper
    reducer_fn = task_module.reducer

    # Connecting to KV_Store RPC Server
    global kv_store_client
    try:
        kv_store_client = get_kv_rpc_client(KV_STORE_IP, KV_STORE_PORT)
        time.sleep(5)
        if not kv_store_client:
            print("Unable to connect to KV Store!\nExiting..")
            sys.exit()
    except Exception as e:
        print("KV Connection Error:",e)

    print("\nSuccessfully connected to KV Store RPC Server!")

    # Starting new socket server and binding it to PORT
    print("Initializing Master Socket..")
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.bind((MASTER_IP, MASTER_PORT))
    master_socket.listen(1)
    # """

    # =============== Mapping ===============
    time.sleep(5)
    # Start Mappers
    print("\n=============== Starting Mappers ===============")
    for i in range(mapper_count):
    
        mapper_id = f"mapper-{str(i)}"
        mapper = MapperThread(mapper_id, MASTER_IP, MASTER_PORT, KV_STORE_IP, KV_STORE_PORT)
        mapper.start()
        print(f"Started {mapper_id}")
        time.sleep(2)

    print("=============== Partitioning input data ===============")
    chunk_keys = []
    if application_task == "word_count":
        # Parsing, Cleaning and Storing Input file into KV Store
        # Input File Path
        input_path = config["input_file1"]
        print(f"Loading input data from..{input_path}")

        input_text = open(input_path, "r").read()
        clean_text = clean_data(input_text)

        
        data_chunks = split_text(clean_text, mapper_count)
        for idx, chunk in enumerate(data_chunks):
            key = "chunk"+str(idx)
            chunk_length = len(chunk)
            print(f"Storing chunk {idx} of length {str(chunk_length)} in kv_store.")
            set_command = f"SET {key} {str(chunk_length)} {chunk}"
            kv_store_client.client_handler("master", set_command)
            chunk_keys.append(key)
    
    if application_task == "inverted_index":

        f1 = config["input_file1"]
        f2 = config["input_file2"]
        
        f1_meta = f1.replace(".txt","").strip()
        f2_meta = f2.replace(".txt","").strip()

        # Parsing file-1
        input_text1 = open(f1, "r").read()
        clean_text1 = clean_data(input_text1)

        # Parsing file-2
        input_text2 = open(f2, "r").read()
        clean_text2 = clean_data(input_text2)

        # Assigning mappers for both files
        total_length = len(clean_text1) + len(clean_text2)
        factor = total_length//mapper_count

        batch1 = len(clean_text1)//factor
        batch2 = mapper_count - batch1

        data_chunks1 = split_text(clean_text1, batch1)
        offset = 0
        for idx, chunk in enumerate(data_chunks1):
            key = f"{f1_meta}-chunk{idx}-{offset}"
            chunk_length = len(chunk)
            print(f"Storing chunk {idx} of length {str(chunk_length)} in kv_store.")
            set_command = f"SET {key} {str(chunk_length)} {chunk}"
            kv_store_client.client_handler("master", set_command)
            chunk_keys.append(key)
            offset += chunk_length
        
        

        data_chunks2 = split_text(clean_text2, batch2)
        offset = 0
        for idx, chunk in enumerate(data_chunks2):
            key = f"{f2_meta}-chunk{idx}-{offset}"
            chunk_length = len(chunk)
            print(f"Storing chunk {idx} of length {str(chunk_length)} in kv_store.")
            set_command = f"SET {key} {str(chunk_length)} {chunk}"
            kv_store_client.client_handler("master", set_command)
            chunk_keys.append(key)
            token_count = len(chunk.split())
            offset += token_count


    print("Accepting connections from Mapper Sockets..")
    mapper_threads = []
    i = 0
    while i < mapper_count:
        (client_socket, addr) = master_socket.accept()
        thread = MasterThread(client_socket, mapper_fn=mapper_fn)
        thread.start()
        mapper_threads.append(thread)
        i+=1

    # =============== Barrier ================
    # Distributed Barrier for Mappers
    while state["mapper_ack"] != mapper_count:
        print("Waiting for Mappers to finish task..")
        time.sleep(5)

    print("\n=============== Mapping Task Finished! ===============")   


    # =============== Reducing ===============
    print("")
    mapper_output = kv_store_client.client_handler("master","GET mapper_files")
    reducer_keys = split_list(mapper_output, reducer_count)

    print("\n=============== Starting Reducers ===============")
    # Start Reducers
    for i in range(reducer_count):
    
        reducer_id = f"reducer-{str(i)}"
        reducer = ReducerThread(reducer_id, MASTER_IP, MASTER_PORT, KV_STORE_IP, KV_STORE_PORT)
        reducer.start()
        print(f"Started..{reducer_id}")
        time.sleep(2)  

    reducer_threads = []
    i = 0
    while i < reducer_count:
        (client_socket, addr) = master_socket.accept()
        thread = MasterThread(client_socket, reducer_fn=reducer_fn)
        thread.start()
        reducer_threads.append(thread)
        i+=1
    
    while state["reducer_ack"] != reducer_count:
        time.sleep(2)
        print("Waiting for Reducers to finish task..")

    print("Reducing Task Finished!")


    # The following line is used to clear kv_store storage files.
    # Needs to be done before starting a new process.
    kv_store_client.reset_server()
