import json
# from jsonrpclib.SimpleJSONRPCServer import SimpleJSONRPCServer
from jsonrpclib.SimpleJSONRPCServer import PooledJSONRPCServer
from jsonrpclib.threadpool import ThreadPool

global KV_STORE_FILE
global KV_STORE_DICT

KV_STORE_FILE = "kv_store.json"
KV_STORE_DICT = {}

def update_kv_store():
    """
        updating kv_store json file with latest changes
    """
    global KV_STORE_FILE
    global KV_STORE_DICT
    f1 = open(KV_STORE_FILE,"r")
    try:
        data = json.load(f1)
    except json.decoder.JSONDecodeError:
        data = {}
    f1.close()

    f2 = open(KV_STORE_FILE,"w")
    data.update(KV_STORE_DICT)
    json.dump(data, f2)
    f2.close()

def reset_server():
    global KV_STORE_FILE
    global KV_STORE_DICT
    KV_STORE_DICT = {}
    open(KV_STORE_FILE,"w").close()
    print("Resetting KV Store internals.")
        
def command_parser(command):
    
    if not command: 
        return []
    
    args = command.split()
    if len(args) < 4:
        return []
    
    data = " ".join(args[3:])
    str_len_given = int(args[2])
    str_len_actual = len(data)
    
    if str_len_actual > str_len_given:
        return []

    return [args[1], data]

def get_filename(text):
    
    numbers = "0123456789"
    char = text.strip()[0]
    char = char.lower()
    if char in numbers:
        return str(abs(hash("1"))) + ".txt"
    f_name = str(abs(hash(char))) + ".txt"
    return f_name

def set_handler(key, val, insert=True):

    global KV_STORE_FILE
    global KV_STORE_DICT
    
    with open(KV_STORE_FILE, "r+") as f:
        try:
            data = json.load(f)
        except json.decoder.JSONDecodeError:
            data = {}
        
        if insert:
            KV_STORE_DICT[key] = val
            data.update({key:val})
        else:
            if key in data:
                KV_STORE_DICT[key].append(val)
                data[key].append(val)
            else:
                KV_STORE_DICT[key] = [val]
                data[key] = [val]
        
        f.seek(0)
        json.dump(data, f)
        f.close()

def get_handler(command):

    global KV_STORE_FILE
    
    args = command.split()
    key = args[1]

    try:
        data = json.load(open(KV_STORE_FILE, "r"))
    except:
        print("Error loading JSON file.")
        return None
    
    val = data.get(key, None)
    if not val:
        print("Key Value Not Found")
        return "Not Found"

    return val
    
    
def client_handler(resource_id, command):
    
    global KV_STORE_FILE
    global KV_STORE_DICT

    if not command:
        return "Empty Request"
    
    if "mapper" in resource_id:
        
        # handle mapper code
        if "SET" in command:
            args = command_parser(command)
            if not args:
                return "NOT STORED"
            
            # Returns filename based on hash
            f_name = get_filename(args[0])
            with open("data/"+f_name,"a") as f:
                
                data = (args[0], args[1])
                f.write(str(data)+"\n")
                f.close()
            
            if "mapper_files" in KV_STORE_DICT:
                if f_name not in KV_STORE_DICT["mapper_files"]:
                    KV_STORE_DICT["mapper_files"].append(f_name)
            else:
                KV_STORE_DICT["mapper_files"] = [f_name]
            
            return "STORED"
                
        if "GET" in command:
            return get_handler(command)
            
    elif "reducer" in resource_id:
        # Reducer code
        
        # handle mapper code
        if "SET" in command:
            args = command_parser(command)
            if not args:
                return "NOT STORED"

            f_name = "reducer_output.txt"
            with open(f_name,"a") as f:
                
                data = (args[0], args[1])
                f.write(str(data)+"\n")
                f.close()
            
        if "GET" in command:
            args = command.split()
            key = args[1]
            # if key not in KV_STORE_DICT["mapper_files"]:
            #     return "Not Found"
            mapper_file = open("data/"+key,"r")
            data = mapper_file.read()
            return data
            
    elif resource_id == "master":
        
        update_kv_store()
        # master
        if "SET" in command:
            args = command_parser(command)
            if not args:
                return "NOT STORED"
            set_handler(args[0], args[1])
            
            
        if "GET" in command:
            return get_handler(command)
        
    else:
        print("Unknown resource_id:", resource_id)

# def start_rpc_server(IP, PORT):
#     global rpc_server
#     rpc_server = SimpleJSONRPCServer((IP, PORT))
#     rpc_server.register_function(client_handler)
#     rpc_server.register_function(stop_server)
#     print(f"KV STORE RPC Server Started on: {IP}:{str(PORT)}")
#     rpc_server.serve_forever()

# def stop_server():
#     print("RPC Shutdown request received..")
#     global rpc_server
#     rpc_server.shutdown()
#     rpc_server.server_close()
#     sys.exit()

if __name__ == "__main__":

    config = json.load(open("config.json","r"))

    IP = config["kv_store"]["IP"]
    PORT = config["kv_store"]["PORT"]
    
    request_pool = ThreadPool(max_threads=50, min_threads=10)
    request_pool.start()

    rpc_server = PooledJSONRPCServer((IP, PORT), thread_pool=request_pool)
    rpc_server.register_function(client_handler)
    rpc_server.register_function(reset_server)
    print(f"KV STORE RPC Server Started on: {IP}:{str(PORT)}")
    try:
        rpc_server.serve_forever()
    finally:
        # Stop the thread pools
        request_pool.stop()