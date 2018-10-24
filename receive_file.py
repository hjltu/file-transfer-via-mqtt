#!/usr/bin/env python3

"""
    receive file hjltu@ya.ru
    payload is json:
    "timeid":       message ID
    "filename":     file name
    "filesize":     "filename" size
    "filehash":     "filename" hash (md5)
    "chunkdata":    chunk of the "filename"
    "chunkhash":    hash of the "chunkdata" (md5)
    "chunknumber":  number of "chunkdata", numbered from (0 - null,zero)
    "encode":       "chunkdata" encoding type (base64)
    "end":          end of message (True - end)

    Usage: receive_file.py dir
"""

import os,glob,sys,time,_thread
import json, binascii,base64, hashlib
import paho.mqtt.client as mqtt

SUBTOPIC="/file"
PUBTOPIC=SUBTOPIC+"/status"
TEMPDIR="temp"
client = mqtt.Client()  # mqtt client

def cleanJson(msg):
    return json.dumps(msg)#.replace("\n", "")

def my_exit(err):   # exit programm
    os._exit(err)
    os.kill(os.getpid)

def my_md5(fname):  # calculate md5sum
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def my_temp_file(mydata,myhash,mynumber,timeid,filename):
    """ save data to temp file
        and send recieved chunknumber
    """
    if hashlib.md5(mydata.encode()).hexdigest() == myhash:
        fname=TEMPDIR+"/"+str(timeid)+"_"+filename+"_.temp"
        f=open(fname, "ab")
        if mynumber==0:
            f=open(fname, "wb")
        try:
            f.write(base64.b64decode(mydata))
        except Exception as e:
            print("ERR: write file",fname,e)
            return 1
        finally:
            f.close()
        print("saved chunk",mynumber,"to",fname)
        client.publish(PUBTOPIC, cleanJson({"chunknumber": mynumber}))

def my_check_temp_files(filename,timeid,filehash):
    """ check temp file and rename to original
    """
    for l in os.listdir(TEMPDIR):
        nameid=l.split("_")[0]
        if nameid == timeid:
            if my_md5(TEMPDIR+"/"+l) == filehash:
                os.rename(TEMPDIR+"/"+l,TEMPDIR+"/"+filename)
    for f in glob.glob("*.temp"):
        os.remove(f)
    print("OK: saved file",filename)

def my_event(top,msg,qos,retain):
    """ convert msg to json,
        send data to file
    """
    try:
        if type(msg) is bytes:
            msg=msg.decode()
        j = json.loads(msg)
    except Exception as e:
        print("ERR: msg2json",e)
        my_exit(2)
    try:
        if j["end"]==False:
            my_temp_file(j["chunkdata"],j["chunkhash"],j["chunknumber"],j["timeid"],j["filename"])
        if j["end"]==True:
            my_check_temp_files(j["filename"],j["timeid"],j["filehash"])
            my_exit(0)
    except Exception as e:
        print("ERR: parse json",e)
        my_exit(3)

def on_connect(client, userdata, flags, rc):
    print("OK Connected with result code "+str(rc))
    client.subscribe(SUBTOPIC,qos=0)
    print("Subscribe: " + SUBTOPIC)

def on_message(client, userdata, msg):
    _thread.start_new_thread(my_event,(msg.topic,msg.payload,msg.qos,msg.retain))

def main():
    if not os.path.exists(TEMPDIR):
        try:
            os.makedirs(TEMPDIR)
        except:
            print("ERR create dir "+TEMPDIR)
            return 1
    #print(__doc__)
    client.connect("192.168.0.10",1883,60)
    #client.connect("localhost",1883,60)
    #client.connect("test.mosquitto.org")
    #client.connect("broker.hivemq.com",1883,60)
    client.on_connect = on_connect
    client.on_message = on_message
    client.loop_forever()


if __name__ == "__main__":
    main()

