##################################################
##### Syslog Server & Write to ElasticSearch (6.3.1)
##### Author: Gmoz Shih
##################################################

import requests
import socketserver
import threading
import queue
import datetime
import json

dataQueue = queue.Queue()
esQueue = queue.Queue()


class SyslogUDPHandler(socketserver.BaseRequestHandler):

    def handle(self):
        rawdata = bytes.decode(self.request[0].strip())
        socket = self.request[1]
        syslogBody = str(rawdata)
        print("%s : " % self.client_address[0], syslogBody)
        dataQueue.put((self.client_address[0], syslogBody, datetime.datetime.now()))


def dayStrToDateDay(str="1970 01 01 00:00:00"):
    return datetime.datetime.strptime(str, "%Y %b %d %H:%M:%S")


# put data to ES
def esWorker():
    headers = {'Content-Type': 'application/json'}
    while True:
        result = ""
        try:
            payload = esQueue.get()
            r = requests.post("http://127.0.0.1:9200/syslog/doc/", data=payload, headers=headers)
            result = r.json()
        except Exception as e:
            print("Put to ES ERROR:", e)
            print(result)


# parse syslog to json
def dataWorker():
    print("Worker Start")
    while True:
        try:
            data = dataQueue.get()
            syslog = {}

            # Sample: <29>Aug  2 11:06:44 N310500 flcdlock: 105: 已重新整理「裝置鎖定原則」。
            ip = data[0]
            content = data[1]
            now = data[2]
            offset = content.index(">") + 1

            # parse PRI
            pri = int(content.split(">")[0].replace("<", ""))
            severity = pri & 0x07
            facility = (pri >> 3) & 0x1f

            # parse time
            mm = content[offset:offset + 3]
            dd = content[offset + 4:offset + 6].replace(" ", "")
            time = content[offset + 7:offset + 15]
            body = content[offset + 16:]
            syslogTime = dayStrToDateDay(str(now.year) + " " + mm + " " + dd + " " + time).strftime("%Y-%m-%d %H:%M:%S")
            receivedTime = str(now.strftime("%Y-%m-%d %H:%M:%S"))

            # to json
            syslog["sourceIP"] = ip
            syslog["receivedTime"] = receivedTime
            syslog["syslogTime"] = syslogTime
            syslog["severity"] = severity
            syslog["facility"] = facility
            syslog["body"] = body
            syslog["raw"] = content
            # print(json.dumps(syslog))
            esQueue.put(json.dumps(syslog))
        except Exception as e:
            print("Parse ERROR ", e)


if __name__ == "__main__":
    dt = threading.Thread(target=dataWorker)
    dt.start()

    et = threading.Thread(target=esWorker)
    et.start()

    print("Syslog server start")
    server = socketserver.UDPServer(("0.0.0.0", 514), SyslogUDPHandler)
    server.serve_forever(poll_interval=0.1)

    dt.join()
    et.join()
