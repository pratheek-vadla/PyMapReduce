import re
import pickle
from jsonrpclib import ServerProxy

def serialize_fn(fn):
    """
        Serializing given function: fn
    """
    fn_str = pickle.dumps(fn)
    return fn_str

def deserialize_fn(fn_str):
    """
        De-Serializing given function binary_str: fn_str
    """
    fn = pickle.loads(fn_str)
    return fn

def clean_data(text):
    
    clean_text = text.replace("\n"," ").replace("\t"," ").replace("\r", " ")
    clean_text = re.sub(r'[^A-Za-z0-9]+'," ", clean_text)
    clean_text = re.sub(r'\s+'," ",clean_text).strip()
    return clean_text

def split_text(input_text, chunks):

    out = []
    chunk_size = len(input_text)//chunks
    offset = 0
    counter = 0
    position_offset = 0
    while counter < chunks and offset < len(input_text):
        
        start = offset
        end = offset + chunk_size
        # Ensuring that the string is split at whitespace, not in between word.
        while end < len(input_text) and input_text[end] != " ":
            end += 1
        temp = input_text[start:end].strip()
        token_count = len(temp.split())
        out.append({
            "chunk":temp,
            "offset":token_count
        })
        offset = end 
        counter += 1
        position_offset += token_count
    return out

def split_list(a, n):
    """
        Splits given list into almost n equal parts
        ack: Referred from Stack Overflow
    """
    k, m = divmod(len(a), n)
    return [a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

def get_kv_rpc_client(IP, PORT):
    
    try:
        conn_str = f"http://{IP}:{str(PORT)}"
        client = ServerProxy(conn_str)
        return client
    except Exception as e:
        print("Error:",e)
        return None