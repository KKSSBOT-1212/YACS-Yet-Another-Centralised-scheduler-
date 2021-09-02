import threading
import socket
import sys
import time
import json

ExecutionPool ={}
lock = threading.Lock()
TaskCompletionTime = {}
TCT = open("TCT.txt","a")

def SendAck():
	print("WORKER WILL SEND ACK TO MASTER ON PORT 5001")
	while True:
		l = list(ExecutionPool.items())
		n = len(l)
		if n == 0:
			continue
		else:
			for i,j in l:
				if j == 0:
					sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					server_address = ('localhost' ,5001)
					sock.connect(server_address)
#					print("now connected")
					message = str(i)
					print(message+"Task has been finished")
					sock.sendall(message.encode())
#					print("sending the message to PORT 5001")
#					print(ExecutionPool)
					del ExecutionPool[i]
#					TaskCompletionTime[i] = time.time() - TaskCompletionTime[i]
					finishing_time = time.time()
					TCT.write(sys.argv[-1]+','+str(i)+","+str(TaskCompletionTime[i])+","+str(finishing_time)+"\n")
					TCT.flush()
#					print("closing connection")
					sock.close()
				else:
#					print("sum")
					ExecutionPool[i] -=1
			time.sleep(1)
			


def RecieveJobs(port,workerId):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('localhost', int(port))
    sock.bind(server_address)
    sock.listen(5)
    print("Listening for tasks from the master on port "+str(port)+" WorkerId "+str(workerId))
    while True:
#    	print("Listening for tasks from the master on port "+str(port)+" WorkerId "+str(workerId))
    	connection, client_address = sock.accept()
    	data = connection.recv(1024)
    	req = data.decode()
    	if len(req):
    		lock.acquire()
    		task = json.loads(req)
    		print(task,"recieved")
    		ExecutionPool[str(task['task_id'])] = task['duration']
    		TaskCompletionTime[str(task['task_id'])] = time.time()
    		lock.release()
    	else:
    		pass



if __name__ == '__main__':
	if(len(sys.argv)!=3):
		print("Usage: python worker.py port workerid")
		exit()
	port = int(sys.argv[1])
	WorkerId = sys.argv[2]
	RecieveJobsThread = threading.Thread(target=RecieveJobs,args = (port,WorkerId))
	RecieveJobsThread.start()
	SendAckThread = threading.Thread(target = SendAck)
	SendAckThread.start()
