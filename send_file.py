#!/usr/bin/env python3

"""
    send file over MQTT hjltu@ya.ru
    payload is json:
    "timeid":       message ID
    "filename":     file name
    "filesize":     "filename" size
    "filehash":     "filename" hash (md5)
    "chunkdata":    chunk of the "filename"
    "chunksize":    size of the "chunkdata" is 99
    "chunkhash":    hash of the "chunkdata" (md5)
    "chunknumber":  number of "chunkdata", numbered from (0 - null,zero)
    "encode":       "chunkdata" encoding type (base64)
    "end":          end of message (True - end)

    Usage: send_file.py file
"""

import os,sys,time,json,threading
import hashlib,base64
import paho.mqtt.client as mqtt

lock=threading.Lock()
client = mqtt.Client()

PUBTOPIC="/file"
SUBTOPIC=PUBTOPIC+"/status"
CHUNKSIZE=9999
chunknumber=0

def cleanJson(msg):
    return json.dumps(msg)#.replace("\n", "")

def my_exit(err):
    os._exit(err)
    os.kill(os.getpid)

def my_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def my_publish(msg):
    try:
        #print("send:",msg,"\n")
        client.publish(PUBTOPIC, cleanJson(msg),qos=0)
        print("pub: msg length =",sys.getsizeof(msg))
    except Exception as e:
        print("ERR: publish",e)

def my_send(myfile):
    """ split, send chunk and wait lock release
    """
    global chunknumber
    time.sleep(2)   # pause for mqtt subscribe
    timeid=str(int(time.time()))
    filesize = os.path.getsize(myfile)
    filehash=my_md5(myfile)

    payload={
    "timeid": timeid, \
    "filename": myfile, \
    "filesize": filesize, \
    "filehash": filehash, \
    "encode": "base64", \
    "end": False}

    with open(myfile,'rb') as f:
        while True:
            chunk=f.read(CHUNKSIZE)
            if chunk:
                data=base64.b64encode(chunk)
                payload.update({ \
                "chunkdata":data.decode(), \
                "chunknumber":chunknumber, \
                "chunkhash":hashlib.md5(data).hexdigest(),
                "chunksize":len(chunk)})
                my_publish(payload)
                lock.acquire()
                chunknumber+=1
            else:
                del payload["chunkdata"]
                del payload["chunknumber"]
                del payload["chunkhash"]
                del payload["chunksize"]
                payload.update({ \
                "end":True})
                print("last message")
                my_publish(payload)
                break
    time.sleep(1)
    my_exit(0)

def my_event(top,msg):
    """ receive confirmation to save chunk
    and release lock for next msg
    """
    global chunknumber
    try:
        j=json.loads(msg.decode())
    except Exception as e:
        print("ERR: json2msg",e)
        my_exit(2)
    try:
        if j["chunknumber"]==chunknumber:
            lock.release()
    except Exception as e:
        print("ERR: in json",e)
        my_exit(3)

def on_connect(client, userdata, flags, rc):
    print("OK Connected with result code "+str(rc))
    client.subscribe(SUBTOPIC)
    print("subscribe to:",SUBTOPIC)

def on_message(client,userdata,msg):
    ev=threading.Thread(target=my_event,args=(msg.topic,msg.payload))
    ev.daemon=True
    ev.start()

def main(myfile="test.txt"):
    tm=time.time()
    if not os.path.isfile(myfile):
        print("ERR: no file",myfile)
        return 1
    #client.connect("localhost",1883,60)
    #client.connect("broker.hivemq.com",1883,60)
    client.connect("192.168.0.10",1883,60)
    #client.connect("test.mosquitto.org")
    client.on_connect = on_connect
    client.on_message = on_message
    my_thread=threading.Thread(target=my_send,args=(myfile,))
    my_thread.daemon=True
    my_thread.start()
    client.loop_forever()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print(__doc__)
        main()

