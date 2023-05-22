def mapper(mapper_id, kv_client, cache_key):
    """
        returns: <status_code>
            status_code
                200 - Successfully completed mapping task
                108 - Failed while processing
                300 - cache_key not found in KV Store
    """
    token_counter = 0
    error_flag = False
    try:
        data = kv_client.client_handler(mapper_id, f"GET {cache_key}")
        if not data or data == "Not Found":
            print(f"{mapper_id} Error: Key:{cache_key} Not Found")
            return f"300:{token_counter}"
    except Exception as e:
        # print("Timeout Exception",e)
        pass

    # Document to which the text belongs to
    # cache_key: doc1-chunk1-1200
    meta = cache_key.split("-")
    doc_id = meta[0]
    offset = int(meta[2])

    tokens = data.split()
    for idx, token in enumerate(tokens):
        val = f"{(doc_id,str(offset+idx))}"
        val_len = len(val)
        set_command = f"SET {token} {val_len} {val}"
        try:
            response = kv_client.client_handler(mapper_id, set_command)
            if response == "STORED":
                token_counter += 1
            else:
                error_flag = True
        except Exception as e:
            # print("Mapper:",e)
            pass

    if not error_flag:
        return f"200:{token_counter}"

    return f"108:{token_counter}"

def reducer(reducer_id, kv_client, cache_keys):
    import time
    """
        returns: <status_code>
            status_code
                200 - Successfully completed reducing task
                108 - Failed while processing
                300 - cache_key not found in KV Store
    """
    error_flag = False
    for key in cache_keys:
        try:
            data = kv_client.client_handler(reducer_id, f"GET {key}")
            if not data:
                error_flag = True
                continue
        except Exception as e:
            pass

        data = data.split("\n")
        data = [eval(x) for x in data if x != ""]
        out = {}
        for item in data:
            key,val = item
            if key in out:
                out[key].append(val)
            else:
                out[key] = [val]
                
        for k,v in out.items():
            v = " ".join(v)
            str_len = len(v)
            data = f"SET {k} {str_len} {v}"
            try:
                response = kv_client.client_handler(reducer_id, data)
            except Exception as e:
                # print("Reducer:",e)
                pass

    if not error_flag:
        return 200

    return 108