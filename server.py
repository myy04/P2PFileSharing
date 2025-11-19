#!/usr/bin/python3

import socket
import threading
import os
import sys
import hashlib

CHUNK_SIZE = 100
MESSAGE_SIZE = 256
MY_PEER_NAME = "NO_NAME"

def getFileList():
    file_names = os.listdir("served_files")
    files = [f for f in file_names if os.path.isfile(os.path.join("served_files", f)) and f.endswith(".txt")]
    
    print("Files: ", files)
    return files

def updateSettingsFile(id, ip, port):
    peer_list = {}
    with open("../peer_settings.txt", "r") as file:
        for line in file.readlines():
            peer_info = line.split()

            peer_list.update({
                peer_info[0] : (peer_info[1], peer_info[2])
            })
        
    peer_list.update({
        id : (ip, port)
    })

    with open("../peer_settings.txt", "w") as file:
        for peer_id in peer_list.keys():
            peer_ip, peer_port = peer_list[peer_id]
            file.writelines(f"{peer_id} {peer_ip} {peer_port}\n")   


def getChunk(file_name, chunk_id):
    directory = "served_files/" + file_name

    text = ""
    with open(directory, "r") as file:
        text = file.read()

    ret = ""
    for i in range(chunk_id * CHUNK_SIZE, min(len(text), (chunk_id + 1) * CHUNK_SIZE)):
        ret += text[i]

    return ret

def getBytesAmount(file_name):
    return int(os.path.getsize('served_files/' + file_name))

class ServerMain:
    def __init__(self, id):
        self.id = id
        hash_value = int(hashlib.md5(self.id.encode()).hexdigest(), 16)
        port_range = 1000  # ports from 8000 to 8999
        self.port = 8000 + (hash_value % port_range)
        self.ip = socket.gethostname()
        self.downloading_files = {}
        updateSettingsFile(self.id, self.ip, self.port)
    
    
    def serverRun(self):
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.bind((self.ip, self.port))

        IP = socket.gethostname()
        serverSocket.listen()

        while True:
            client = serverSocket.accept()
            t = ServerThread(client, self)
            t.start()


class ServerThread(threading.Thread):
    def __init__(self, client, serverMain):
        threading.Thread.__init__(self)
        self.client = client
        self.serverMain = serverMain
        self.receiving_file = None

    def parseRequest(self, request):
        if len(request) > 50:
            print(f"Client: {request[0:50]}...")
        else:
            print(f"Client: {request}")

        command = request.split(' ')[0]

        if command == "#FILELIST":
            return "200 Files served: " + ' '.join(getFileList())
            
        if command == "#DOWNLOAD":
            file_name = request.split(' ')[1]

            if "chunk" not in request.split(' '):

                if file_name not in getFileList():
                    return f"250 Not serving file {file_name}"

                else:
                    return f"330 Ready to send file {file_name} bytes {getBytesAmount(file_name)}"

            chunk_id = int(request.split(' ')[-1])
        
            return f"200 File {file_name} chunk {chunk_id} {getChunk(file_name, chunk_id)}"

        if command == "#UPLOAD":

            file_name = request.split(' ')[1]

            if "chunk" not in request.split(' '):

                if file_name in self.serverMain.downloading_files.keys():
                    return f"250 Currently receiving file {file_name}"
                
                elif file_name in getFileList():
                    return f"250 Already serving file {file_name}"

                else:
                    bytes = int(request.split(' ')[-1])
                    num_chunks = (bytes + CHUNK_SIZE - 1) // CHUNK_SIZE

                    print(f"Receiving file {file_name} bytes {bytes}")

                    self.receiving_file = file_name

                    self.serverMain.downloading_files.update({file_name : {}})
                    self.serverMain.downloading_files[file_name].update({"Bytes" : bytes})
                    self.serverMain.downloading_files[file_name].update({"Chunks" : num_chunks})
                    self.serverMain.downloading_files[file_name].update({"Chunks Received" : 0})

                    for chunk_id in range(num_chunks):
                        self.serverMain.downloading_files[file_name].update({chunk_id : None})

                    return f"330 Ready to receive file {file_name}"
                
            else:
                chunk_id = int(request.split(' ')[3])
                chunk = ' '.join(request.split(' ')[4:])

                self.serverMain.downloading_files[file_name][chunk_id] = chunk

                self.serverMain.downloading_files[file_name]["Chunks Received"] += 1

                if self.serverMain.downloading_files[file_name]["Chunks Received"] == self.serverMain.downloading_files[file_name]["Chunks"]:
                    
                    data = ""
                    for i in range(self.serverMain.downloading_files[file_name]["Chunks"]):
                        data += self.serverMain.downloading_files[file_name][i]
                    
                    with open("served_files/" + file_name, "w") as file:
                        file.write(data)

                    self.serverMain.downloading_files.pop(file_name)
                 
                    return f"200 File {file_name} received"
                else: 
                    return f"200 File {file_name} chunk {chunk_id} received"



    def run(self):
        connectionSocket, addr = self.client
        
        try:
            
            request = ''
            while request == '':
                request = connectionSocket.recv(MESSAGE_SIZE).decode()

            response = self.parseRequest(request)
        
        
            print(f"Server ({MY_PEER_NAME}): ", response)


            connectionSocket.send(response.encode())

        except socket.error:   
            print(f"TCP Connection to {addr} failed")

            if self.receiving_file != None:
                self.serverMain.downloading_files.pop(self.receiving_file)

        finally:
            connectionSocket.close()



if __name__ == "__main__":
    args = sys.argv
    MY_PEER_NAME = args[1]
    server = ServerMain(MY_PEER_NAME)
    server.serverRun() 
