"""
 * Copyright (c) 2018, Autonomous Networks Research Group. All rights reserved.
 * Developed by:
 * Autonomous Networks Research Group (ANRG)
 * University of Southern California
 * http://anrg.usc.edu/
 *
 * Contributors:
 * Pranav Sakulkar, 
 * Pradipta Ghosh, 
 * Aleksandra Knezevic, 
 * Jiatong Wang, 
 * Quynh Nguyen, 
 * Jason Tran, 
 * H.V. Krishna Giri Narra, 
 * Zhifeng Lin, 
 * Songze Li, 
 * Ming Yu, 
 * Bhaskar Krishnamachari, 
 * Salman Avestimehr, 
 * Murali Annavaram
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or 
 * sell copies of the Software, and to permit persons to whom the Software is 
 * furnished to do so, subject to the following conditions:
 * - Redistributions of source code must retain the above copyright notice, this
 *     list of conditions and the following disclaimers.
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *     this list of conditions and the following disclaimers in the 
 *     documentation and/or other materials provided with the distribution.
 * - Neither the names of Autonomous Networks Research Group, nor University of 
 *     Southern California, nor the names of its contributors may be used to 
 *     endorse or promote products derived from this Software without specific 
 *     prior written permission.
 * - A citation to the Autonomous Networks Research Group must be included in 
 *     any publications benefiting from the use of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH 
 * THE SOFTWARE.
"""

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client as xmlrpclib
import socket
import fcntl
import struct
import multiprocessing
import numpy as np
import sys
import json
import time
import os
import re
import math
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import PatternMatchingEventHandler
import _thread
import subprocess


configs = json.load(open('/centralized_scheduler/config.json'))
CHUNKS = configs['chunks'] 
# MASTER_ID = sys.argv[1]
# SLAVE_ID = sys.argv[2]

all_nodes = os.environ["ALL_NODES"].split(":")
all_nodes_ips = os.environ["ALL_NODES_IPS"].split(":")
node_dict = dict(zip(all_nodes, all_nodes_ips))

# newIps = os.environ['NODE_IPS'].split(':')
# numSlaves = int(os.environ['TOTAL_SLAVES'])

# configs = json.load(open('config/config.json'))
# configs['masterConfigs']['IP'] = newIps[0]
# for i in range(1, numSlaves + 1):
#     configs['slaveConfigs']['slave'+str(i)]['IP'] = newIps[i]


def DFTMatrix(N):
    i,j = np.meshgrid(np.arange(N), np.arange(N), indexing = 'ij') #indexing 'ij' since matrix co-ord.
    omega = np.exp(-2 * math.pi * 1J/N)
    W = np.power(omega,i*j)/math.sqrt(1)
    return W

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', b'eth0')
    )[20:24])

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Register an instance; all the methods of the instance are
# published as XML-RPC methods (in this case, just 'div').
class MyFuncs:
    def __init__(self, token, mvToken):
        self.token = token
        self.mvToken = mvToken
        self.rows = None
        self.lengths = None
        self.replicas = None
        self.isReleased = False

    def accept_matrix(self, rows, lengths, replicas=1):
        self.rows = rows
        self.lengths = lengths
        self.replicas = replicas
        return
    
    def start(self):
        self.token.clear()
        self.token.set()
        return

    def retrieve_matrix(self):
        return self.rows, self.lengths, self.replicas

    def release(self):
        self.isReleased = True
        self.mvToken.clear()
        self.mvToken.set()
        return

    def is_released(self):
        ret = self.isReleased
        self.isReleased = False
        return ret


# class SlaveServer(xmlrpc.server.SimpleXMLRPCServer):
#     allow_reuse_address = True

class SlaveServerProcess(multiprocessing.Process):
    quit = False
    def __init__(self, myIP, myPortNum, token, mvToken):
        multiprocessing.Process.__init__(self)
        self.server = SimpleXMLRPCServer((myIP, int(myPortNum)),requestHandler=RequestHandler, allow_none=True,bind_and_activate=False)
        self.server.allow_reuse_address = True
        self.daemon = True
        self.server.server_bind()
        self.server.server_activate()  
        self.server.register_introspection_functions()
        
        myFuncs = MyFuncs(token, mvToken)
        self.funcs = myFuncs
        self.server.register_instance(myFuncs)

    # def run(self):
    #     self.server.serve_forever()
    def run(self):
        # self.server.serve_forever()
        while not self.quit:
            self.server.handle_request()

    def stop_server(self):
        self.quit = True
        self.server.server_close()
        print("here")

        return 0

def matrixMultiply(matrix, vector, rows, lengths, replicas, mvToken, timeTotal):
    chunks = CHUNKS
    product = None
    print('I am doing extra work')
    print('one replica takes %s seconds' % str(timeTotal))
    for chunk in range(chunks):
        row = rows[chunk]
        length = lengths[chunk]
        for i in range(1, replicas[chunk]):
#             time.sleep(timeTotal[chunk])
            if not chunk:
                product = matrix[row : (row+length), :].dot(vector)
            elif not i:
                product = np.vstack((product, matrix[row : (row+length), :].dot(vector)))
            else:
                matrix[row : (row+length), :].dot(vector)
        print(replicas[chunk])
    mvToken.clear()
    mvToken.set()


class Watcher1():
    
    DIRECTORY_TO_WATCH = '/home/darpa/apps/data/'


    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.observer = Observer()

    def run(self):
        """
            Continuously watching the ``OUTPUT`` folder, if there is a new file created for the current task, copy the file to the corresponding ``INPUT`` folder of the next task in the scheduled node
        """
        event_handler = Handler1()
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except:
            self.observer.stop()
            print("Error")

        self.observer.join()

class Handler1(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        """
            Check for any event in the ``OUTPUT`` folder
        """

        global MASTER_ID,SLAVE_ID,SLAVE_NUM

        print("1) Reached inside matrixMulKernelSlave function")
        master =  node_dict['dftdetector' + str(MASTER_ID)] + ':' + configs['PortNum']
        print("Master:", master)

        #myIP = configs['slaveConfigs']['slave' + ID]['IP']
        myIP = get_ip_address('eth0')
        print("My IP:", myIP)

        myPortNum = configs['PortNum']
        # node_dict['dftslave' + ID]['PortNum']
        encoding = np.array(configs['matrixConfigs']['encoding'])
        print("2) Ports, encoding", myPortNum, encoding)


        print("4) Done loading")
        # Create server
        token = multiprocessing.Event()
        mvToken = multiprocessing.Event()

        
        global MASTER_ID,SLAVE_ID,SLAVE_NUM, master, myIP,myPortNum,encoding,token,myToken,server_process

        
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            if 'product' in event.src_path:
                return None
            print("5) Creating slave server process")
            server_process = SlaveServerProcess(myIP, myPortNum, token, mvToken) 

            server_process.start()
            print('starting slave server process %d...' % server_process.pid)
             
            print("Received file as output - %s." % event.src_path)
            print(event.src_path)
            print(os.path.dirname(os.path.abspath(__file__)))
            new_file = os.path.split(event.src_path)[-1]
            print(new_file)
            matrixFile = os.path.join('../home/darpa/apps/data/',new_file)
            print(matrixFile)
            matrixMulKernelSlave(MASTER_ID,str(SLAVE_ID),SLAVE_NUM,myIP,myPortNum,master,token,mvToken,None,matrixFile)
            print('reach here')
            server_process.stop_server()
            server_process.terminate()

            script = 'kill -9 %d' % server_process.pid
            print(script)
            output = subprocess.check_output(script, shell=True)
            print(output)

            os.remove(event.src_path)
            # ps aux | grep "dftslave.py" | tr -s ' ' | cut -d " " -f 2
        

def run_watch():
    print('Start watching INPUT folder')
    w1 = Watcher1()
    w1.run()

# def matrixMulKernelSlave(MasterID, ID, TP, vector=None, matrix=None):
def matrixMulKernelSlave(MasterID, ID, TP,myIP,myPortNum,master,token,mvToken,vector=None, matrixFile=None):
    # print("1) Reached inside matrixMulKernelSlave function")
    # master =  node_dict['dftdetector' + str(MasterID)] + ':' + configs['PortNum']
    # print("Master:", master)

    # #myIP = configs['slaveConfigs']['slave' + ID]['IP']
    # myIP = get_ip_address('eth0')
    # print("My IP:", myIP)

    # myPortNum = configs['PortNum']
    # # node_dict['dftslave' + ID]['PortNum']
    # encoding = np.array(configs['matrixConfigs']['encoding'])
    # print("2) Ports, encoding", myPortNum, encoding)

    # if matrix is None:
    #     print("3) Loading matrixFile")
    # matrix = np.loadtxt(matrixFile, dtype=int)
    # print("4) Done loading")
    # # Create server
    # token = multiprocessing.Event()
    # mvToken = multiprocessing.Event()

    # matrices = (matrix, matrix)
    # print("5) Creating slave server process")
    # server_process = SlaveServerProcess(myIP, myPortNum, token, mvToken) 

    # server_process.start()
    # print('starting slave server process %d...' % server_process.pid)

    # print('----------------------- DEBUG')
    # print(myIP)
    # print(myPortNum)

    # if matrix is None:
    #     print("3) Loading matrixFile")        
    # matrixFile = '/home/darpa/apps/data/partition%s.mat' % SLAVE_ID
    # time.sleep(5)
    matrix = np.loadtxt(matrixFile, dtype=int)

    print(matrix)

    matrices = (matrix, matrix)

    localProxy = xmlrpclib.ServerProxy('http://' + myIP + ':' + myPortNum, allow_none=True)
    print(localProxy)
    # print(master)
    masterProxy = xmlrpclib.ServerProxy('http://' + master, allow_none=True)
    
    print(masterProxy)
    chunks = CHUNKS
    idx = 0

    
    while True:
        try:
            print(ID)
            print(masterProxy)
            masterProxy.slave_ready(ID)
            break
        except:
            print("master did not start/accept ACK.")
            time.sleep(1)
            pass

    if True:
    #while True:
        print('reach here2')
        matrix = matrices[idx % 2]
        idx += 1
        product = None
        timeTotal = []
        for chunk in range(chunks):
            print(chunk)
            if not chunk:
                token.wait()
                token.clear()
                mvToken.clear()
                localProxy.is_released() 
                vector = DFTMatrix(64) 

                rows, lengths, replicas = localProxy.retrieve_matrix()
                start_time = time.time()
            row = rows[chunk]
            length = lengths[chunk]
            timeB = time.time()
            print(timeB)
            if not chunk:
                product = matrix[row : (row+length), :].dot(vector)
            else:
                product = np.vstack((product, matrix[row : (row+length), :].dot(vector)))
            timeE = time.time()
            print(timeE)
            timeTotal.append(timeE - timeB)
        print(matrix.shape, vector.shape, product.shape)
        mv = multiprocessing.Process(target=matrixMultiply, args=(matrix, vector, rows, lengths, replicas, mvToken, timeTotal))
        mv.start()
        mvToken.wait()
        mvToken.clear()
        mv.terminate()             
        end_time = time.time()
        
        print('slave' + ID + ': time to compute: %f' % (end_time - start_time))
        start_time = end_time
        #if not masterProxy.checkDone():
        if not localProxy.is_released():
            productFile = ''
            productFile = '/home/darpa/apps/data/product%s.mat' % ID

            np.savetxt(productFile, product.view(float))
            masterIP = node_dict['dftdetector' + MasterID]
            cmd = "sshpass -p 'PASSWORD' scp -P 5000 -o StrictHostKeyChecking=no %s %s:%s" % (productFile, masterIP, productFile) 
            #cmd = "scp %s %s:%s" % (productFile, masterIP, productFile) 
            os.system(cmd)
            end_time = time.time()
            print('slave' + ID + ': time to "send" result: %f' % (end_time - start_time))
            try:
                masterProxy.accept_product(productFile, 'slave' + ID)
            except:
                print('slave'+ ID + ': I am too slow and the master is already killed')
        else:
            print('slave'+ ID + ': I am too slow and the master has what it needs')
        
    # server_process.terminate()

# def matrixMulKernelSlave(MasterID, ID, TP, vector=None, matrix=None):
#     print("1) Reached inside matrixMulKernelSlave function")
#     master =  node_dict['dftdetector' + str(MasterID)] + ':' + configs['PortNum']
#     print("Master:", master)

#     #myIP = configs['slaveConfigs']['slave' + ID]['IP']
#     myIP = get_ip_address('eth0')
#     print("My IP:", myIP)

#     myPortNum = configs['PortNum']
#     # node_dict['dftslave' + ID]['PortNum']
#     encoding = np.array(configs['matrixConfigs']['encoding'])
#     print("2) Ports, encoding", myPortNum, encoding)

#     k, n = encoding.shape
#     myID = int(ID)
#     matrixFile = '/home/darpa/apps/data/partition%s.mat' % ID
#     while not os.path.exists(matrixFile):
#         time.sleep(2)
#     if matrix is None:
#         print("3) Loading matrixFile")
#         matrix = np.loadtxt(matrixFile, dtype=int)
#     print("4) Done loading")
#     # Create server
#     token = multiprocessing.Event()
#     mvToken = multiprocessing.Event()

#     matrices = (matrix, matrix)
#     print("5) Creating slave server process")
#     server_process = SlaveServerProcess(myIP, myPortNum, token, mvToken) 

#     server_process.start()
#     print('starting slave server process %d...' % server_process.pid)

#     # print('----------------------- DEBUG')
#     # print(myIP)
#     # print(myPortNum)

#     localProxy = xmlrpclib.ServerProxy('http://' + myIP + ':' + myPortNum, allow_none=True)
#     # print(localProxy)
#     # print(master)
#     masterProxy = xmlrpclib.ServerProxy('http://' + master, allow_none=True)
    
#     # print(masterProxy)
#     chunks = CHUNKS
#     idx = 0

    
#     while True:
#         try:
#             # print(ID)
#             # print(masterProxy)
#             masterProxy.slave_ready(ID)
#             break
#         except:
#             print("master did not start/accept ACK.")
#             time.sleep(1)
#             pass

#     if True:
#     #while True:
#         matrix = matrices[idx % 2]
#         idx += 1
#         product = None
#         timeTotal = []
#         for chunk in range(chunks):
#             if not chunk:
#                 token.wait()
#                 token.clear()
#                 mvToken.clear()
#                 localProxy.is_released() 
#                 vector = DFTMatrix(64) 

#                 rows, lengths, replicas = localProxy.retrieve_matrix()
#                 start_time = time.time()
#             row = rows[chunk]
#             length = lengths[chunk]
#             timeB = time.time()
#             if not chunk:
#                 product = matrix[row : (row+length), :].dot(vector)
#             else:
#                 product = np.vstack((product, matrix[row : (row+length), :].dot(vector)))
#             timeE = time.time()
#             timeTotal.append(timeE - timeB)
#         print(matrix.shape, vector.shape, product.shape)
#         mv = multiprocessing.Process(target=matrixMultiply, args=(matrix, vector, rows, lengths, replicas, mvToken, timeTotal))
#         mv.start()
#         mvToken.wait()
#         mvToken.clear()
#         mv.terminate()             
#         end_time = time.time()
#         print('slave' + ID + ': time to compute: %f' % (end_time - start_time))
#         start_time = end_time
#         #if not masterProxy.checkDone():
#         if not localProxy.is_released():
#             productFile = ''
#             productFile = '/home/darpa/apps/data/product%s.mat' % ID

#             np.savetxt(productFile, product.view(float))
#             masterIP = node_dict['dftdetector' + MasterID]
#             cmd = "sshpass -p 'PASSWORD' scp -P 5000 -o StrictHostKeyChecking=no %s %s:%s" % (productFile, masterIP, productFile) 
#             #cmd = "scp %s %s:%s" % (productFile, masterIP, productFile) 
#             os.system(cmd)
#             end_time = time.time()
#             print('slave' + ID + ': time to "send" result: %f' % (end_time - start_time))
#             masterProxy.accept_product(productFile, 'slave' + ID)
#         else:
#             print('slave'+ ID + ': I am too slow and the master has what it needs')
        
#     server_process.terminate()

def main():
    if len(sys.argv) < 4:
        print('incorrect number of arguments')
        print('please provide id of the slave server, N/T to indicate transpose or not') 
        sys.exit(-1)

    print("Reached the first step in main")
    global MASTER_ID,SLAVE_ID,SLAVE_NUM
    MASTER_ID = sys.argv[1]
    SLAVE_ID = int(sys.argv[2]) + 1
    SLAVE_NUM = sys.argv[3]

    # global master, myIP,myPortNum,encoding,token,myToken,server_process

    # print("Master, ID, TP:", MASTER_ID, str(SLAVE_ID), SLAVE_NUM)

    # print("1) Reached inside matrixMulKernelSlave function")
    # master =  node_dict['dftdetector' + str(MASTER_ID)] + ':' + configs['PortNum']
    # print("Master:", master)

    # #myIP = configs['slaveConfigs']['slave' + ID]['IP']
    # myIP = get_ip_address('eth0')
    # print("My IP:", myIP)

    # myPortNum = configs['PortNum']
    # # node_dict['dftslave' + ID]['PortNum']
    # encoding = np.array(configs['matrixConfigs']['encoding'])
    # print("2) Ports, encoding", myPortNum, encoding)


    # print("4) Done loading")
    # # Create server
    # token = multiprocessing.Event()
    # mvToken = multiprocessing.Event()

    # print("5) Creating slave server process")
    # server_process = SlaveServerProcess(myIP, myPortNum, token, mvToken)
    # server_process.start()

    print('Start watching INPUT folder')
    w1 = Watcher1()
    w1.run()

    # matrixMulKernelSlave(MASTER_ID, str(SLAVE_ID), SLAVE_NUM)

if __name__ == '__main__':
    main()   
