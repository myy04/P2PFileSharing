
import socket
import threading
import os
import sys

CHUNK_SIZE = 100
MESSAGE_SIZE = 256
MY_PEER_NAME = "NO_NAME"

def getPeerList():
    settings_file = open("../peer_settings.txt", "r")
    peer_list = {}

    for line in settings_file.readlines():
        peer_info = line.split()
        peer_list.update({peer_info[0] : (peer_info[1], int(peer_info[2]))})
    
    settings_file.close()
    return peer_list

def getFileList():
    file_names = os.listdir("served_files")
    files = [f for f in file_names if os.path.isfile(os.path.join("served_files", f)) and f.endswith(".txt")]
    return files

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


class ClientMain:
    def __init__(self):    
        self.downloading_files = {}

    def requestFileList(self, peers):
        for peer in peers:
            t = ClientThread(peer, self)
            t.requestFileList()

    def requestFileDownload(self, peers, file_name):
        if file_name in getFileList():
            print(f"File {file_name} already exists.")
            return

        print(f"Downloading file {file_name}")

        self.downloading_files.update({file_name : {}})
        self.downloading_files[file_name].update({"Requested" : peers})
        self.downloading_files[file_name].update({"Accepted" : []})
        self.downloading_files[file_name].update({"Rejected" : []})
        self.downloading_files[file_name].update({"Bytes" : 0})

        for peer in peers:
            t = ClientThread(peer, self)
            t.requestFileDownload(file_name)

    def startFileDownload(self, peers, file_name):
        if len(peers) == 0:
            print(f"File {file_name} donwload failed. Peers {' '.join(self.downloading_files[file_name]['Rejected'])} are not serving the file.")
            self.downloading_files.pop(file_name)
            return
        
        num_chunks = (self.downloading_files[file_name]["Bytes"] + CHUNK_SIZE - 1) // CHUNK_SIZE

        self.downloading_files[file_name].update({"Chunks" : num_chunks})
        self.downloading_files[file_name].update({"Chunks Received" : 0})

        for i in range(num_chunks):
            t = ClientThread(peers[i % len(peers)], self)
            t.requestChunkDownload(file_name, i)
        
    def finishFileDownload(self, file_name):
        if file_name not in self.downloading_files.keys():
            print(f"File {file_name} download failed")
            return

        data = ""
        for i in range(self.downloading_files[file_name]["Chunks"]):
            data += self.downloading_files[file_name][i]
        
        with open("served_files/" + file_name, "w") as file:
            file.write(data)

        self.downloading_files.pop(file_name)

        print(f"File {file_name} download success")

    def requestFileUpload(self, peers, file_name):
        if file_name not in getFileList():
            print(f"Peer {MY_PEER_NAME} does not serve file {file_name}")
            return
        
        print(f"Uploading file {file_name}")

        for peer in peers:
            t = ClientThread(peer, self)
            t.requestFileUpload(file_name)


    def run(self):
        while True:
            command = input().strip().split(' ')

            request = command[0]

            if request == "#FILELIST":
                peers = command[1:]

                self.requestFileList(peers)

            if request == "#DOWNLOAD":
                file_name = command[1]
                peers = command[2:]

                self.requestFileDownload(peers, file_name)

            if request == "#UPLOAD":   

                file_name = command[1]
                peers = command[2:]

                self.requestFileUpload(peers, file_name)



class ClientThread(threading.Thread):
    def __init__(self, peer, clientMain):
        threading.Thread.__init__(self)
        self.peer = peer   
        self.clientMain = clientMain
        self.response = ""        
        self.peer_list = getPeerList()

    def sendMessage(self, msg):
        if len(msg) > 30:
            print(f"Client ({MY_PEER_NAME}): {msg[0:30]}...")
        else:
            print(f"Client ({MY_PEER_NAME}): {msg}")

        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            
            if self.peer not in self.peer_list.keys():
                raise socket.error
            
            serverIP, serverPort = self.peer_list[self.peer]
            clientSocket.connect((serverIP, serverPort))
            clientSocket.send(str(msg).encode())

            self.response = ''
            while self.response == '':
                self.response = clientSocket.recv(MESSAGE_SIZE).decode()


        except socket.error:
            self.response = f"250 TCP connection to server {self.peer} failed"

        finally:
            clientSocket.close()

    def getResponse(self):
        while self.response == "":
            continue

        response = self.response
        self.response = ""

        if len(response) > 50:
            print(f"Server ({self.peer}): {response[0:50]}...")
        else:
            print(f"Server ({self.peer}): {response}")

        return response

    def requestFileList(self):
        peer = self.peer
    
        self.sendMessage("#FILELIST")
        response = self.getResponse()

    def requestFileDownload(self, file_name):
        downloading_file = self.clientMain.downloading_files[file_name]
        peer = self.peer

        self.sendMessage(f"#DOWNLOAD {file_name}")
        response = self.getResponse()
        
        status = response.split(' ')[0]

        if status == "330":
            downloading_file["Accepted"] += [peer]
            downloading_file["Bytes"] = int(response.split(' ')[-1])

        else:
            downloading_file["Rejected"] += [peer]

        if len(downloading_file["Accepted"]) + len(downloading_file["Rejected"]) == len(downloading_file["Requested"]):
            self.clientMain.startFileDownload(downloading_file["Accepted"], file_name)

    def requestChunkDownload(self, file_name, chunk_id):   
        if file_name not in self.clientMain.downloading_files.keys():
            return

        downloading_file = self.clientMain.downloading_files[file_name]
        downloading_file.update({chunk_id : None})

        self.sendMessage(f"#DOWNLOAD {file_name} chunk {chunk_id}")
        response = self.getResponse()

        status = response.split(' ')[0]

        if status == "200":
            chunk = ' '.join(response.split(' ')[5:])

            downloading_file[chunk_id] = chunk
            downloading_file["Chunks Received"] += 1

            if downloading_file["Chunks Received"] == downloading_file["Chunks"]:
                self.clientMain.finishFileDownload(file_name)

        else:
            if file_name in self.clientMain.downloading_files.keys(): 
                print(f"File {file_name} chunk {chunk_id} download failed")

                self.clientMain.downloading_files.pop(file_name)


    def requestFileUpload(self, file_name):
        peer = self.peer

        self.sendMessage(f"#UPLOAD {file_name} bytes {getBytesAmount(file_name)}")
        response = self.getResponse()

        status = response.split(' ')[0]

        if status != "330":
            return
        
        num_chunks = (getBytesAmount(file_name) + CHUNK_SIZE - 1) // CHUNK_SIZE

        for chunk_id in range(num_chunks):
            chunk = getChunk(file_name, chunk_id)

            self.sendMessage(f"#UPLOAD {file_name} chunk {chunk_id} {chunk}")
            response = self.getResponse()
            
            status = response.split(' ')[0]

            if status != "200":
                print(f"File {file_name} upload failed")
                return

        print(f"File {file_name} upload success")


def main():
    clientMain = ClientMain()
    clientMain.start()

if __name__ == '__main__':
    args = sys.argv
    MY_PEER_NAME = args[1]
    main()
