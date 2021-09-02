import threading
import time
import socket
import json
import sys
import random

jobs = []
lock = threading.Lock()
ReadyQueue = []

def accept_jobs():
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('localhost', 5000)
	sock.bind(server_address)
	sock.listen(1)
	print("Listening to JOB requests on port 5000")
	while True:
		connection, client_address = sock.accept()
		
		try:
			while True:
				data = connection.recv(1024)
				req = data.decode()
				if len(req):
					arrivaltime = time.time()
					lock.acquire()
					JobDict = json.loads(req)
					jobs.append(JobDict)
					JobArr.write(str(arrivaltime)+','+str(JobDict['job_id'])+'\n')
					JobArr.flush()
					for Mtask in JobDict['map_tasks']:
						ReadyQueue.append([Mtask,0])
					for Rtask in JobDict['reduce_tasks']:
						ReadyQueue.append([Rtask,0])
				else:
					break
				lock.release()
			
		finally:
			connection.close()
			
def ListenToWorkers(NoOfWorkers):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	server_address = ('localhost', 5001)
	sock.bind(server_address)
	sock.listen(NoOfWorkers)
	print("Listening To Worker Request on PORT 5001")
	while True:
		connection, client_address = sock.accept()
		try:
			while True:
				data = connection.recv(1024)
				req = data.decode()
				if len(req):
					if not(AnalyseThread.is_alive()):
						AnalyseThread.start()
					worker = TaskWorker[req]
					print("Received",req,"from",worker)
					AllWorkers[worker-1]['slots'] += 1
					del TaskWorker[req]
				else:
					break
		finally:
			connection.close()
			
			
			
def Random():
	print("USING RANDOM SCHEDULING ALGORITHM")
	while True:
		for task,status in ReadyQueue:
			if '_M' in task['task_id']:
				num = random.randint(1,3)
				while AllWorkers[num-1]['slots'] == 0:
					num = random.randint(1,3)
				if status == 0:
					sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					server_address = ('localhost' ,AllWorkers[num-1]['port'])
					sock.connect(server_address)
					zm = ReadyQueue.index([task,status])
					ReadyQueue[zm][1] = 1
					tasksend = json.dumps(task)
					print("Sending",task['task_id'],"to",num)
					sock.sendall(tasksend.encode())
					sock.close()
					TaskWorker[task['task_id']] = num
#					print("Task worker is updated",TaskWorker)
					AllWorkers[num-1]['slots'] -= 1
#					print("Slots updated",AllWorkers)				
				else:
					continue
			else:
				s = task['task_id'][:-3]
				t = False
				l = list(TaskWorker.keys())
				for x in l:
					if s + '_M' in x:
						t = True
						break
				if t == False:
					numr = random.randint(1,3)
					while AllWorkers[numr -1]['slots'] == 0: 
						numr = random.randint(1,3)
					if status == 0:
						sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
						server_address = ('localhost' ,AllWorkers[numr-1]['port'])
						sock.connect(server_address)
						zr = ReadyQueue.index([task,status])
						ReadyQueue[zr][1] = 1
						tasksend = json.dumps(task)
						print("Sending",task['task_id'],"to",numr)
						sock.sendall(tasksend.encode())
						sock.close()
						TaskWorker[task['task_id']] = numr
#						print("Task worker is updated",TaskWorker)
						AllWorkers[numr-1]['slots'] -= 1
#						print("Slots updated",AllWorkers)
					else:
						continue
						
def RR():
	print("USING ROUND ROBIN TO SCHEDULE TASKS")
	while True:
		for task,status in ReadyQueue:
			if '_M' in task['task_id']:
				for ctr in range(NoOfWorkers):
					if AllWorkers[ctr]['slots'] != 0:
						if status == 0:
							sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
							server_address = ('localhost' ,AllWorkers[ctr]['port'])
							sock.connect(server_address)
							ctrm = ReadyQueue.index([task,status])
							ReadyQueue[ctrm][1] = 1
							tasksend = json.dumps(task)
							print("sending",task['task_id'],"to",ctr+1)
							sock.sendall(tasksend.encode())
							sock.close()
							TaskWorker[task['task_id']] = ctr +1
#							print("Task worker is updated",TaskWorker)
							AllWorkers[ctr]['slots'] -= 1
							break
#							print("Slots updated",AllWorkers)
						else:
							pass
					else:
						continue
			else:
				srr = task['task_id'][:-3]
				trr = False
				lrr = list(TaskWorker.keys())
				for xrr in lrr:
					if srr + '_M' in xrr:
						trr = True
						break
				if trr == False:
					for ctrred in range(NoOfWorkers):
						if AllWorkers[ctrred]['slots'] != 0:
							if status == 0:
								sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
								server_address = ('localhost' ,AllWorkers[ctrred]['port'])
								sock.connect(server_address)
								ctrr = ReadyQueue.index([task,status])
								ReadyQueue[ctrr][1] = 1
								tasksend = json.dumps(task)
								print("sending",task['task_id'],"to",ctrred+1)
								sock.sendall(tasksend.encode())
								sock.close()
								TaskWorker[task['task_id']] = ctrred +1
#								print("Task worker is updated",TaskWorker)
								AllWorkers[ctrred]['slots'] -= 1
#								print("Slots Increasing",AllWorkers)
								break
							else:
								pass
						else:
							continue
	print("RR DONE")
	
	
def LL():
	print("USING LEAST LOADED SCHEDULING ALGORITHM")
	while True:
		for task,status in ReadyQueue:
			CpyAllWorkers = sorted(AllWorkers,key=lambda w:w['slots'],reverse=True)
			while CpyAllWorkers[0]['slots'] == 0:
				CpyAllWorkers = sorted(AllWorkers,key = lambda w:w['slots'],reverse = True)
				print("slots full")
				time.sleep(1)
			if '_M' in task['task_id']:
				if status == 0:
					sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					server_address = ('localhost' ,CpyAllWorkers[0]['port'])
					sock.connect(server_address)
					ctrlm = ReadyQueue.index([task,status])
					ReadyQueue[ctrlm][1] = 1
					tasksend = json.dumps(task)
					print("sending",task['task_id'],"to",CpyAllWorkers[0]['worker_id'])
					sock.sendall(tasksend.encode())
					sock.close()
					TaskWorker[task['task_id']] = CpyAllWorkers[0]['worker_id']
#					print("Task worker is updated",TaskWorker)
#					CpyAllWorkers[0]['slots'] -= 1
					updm = AllWorkers.index(CpyAllWorkers[0])
					AllWorkers[updm]['slots'] -= 1
#					print("Slots decreased",AllWorkers)
				else:
					continue
			else:
				sll = task['task_id'][:-3]
				tll = False
				lll = list(TaskWorker.keys())
				for xll in lll:
					if sll + '_M' in xll:
						tll = True
						break
				if tll == False:
					if status == 0:
						sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
						server_address = ('localhost' ,CpyAllWorkers[0]['port'])
						sock.connect(server_address)
						ctrlr = ReadyQueue.index([task,status])
						ReadyQueue[ctrlr][1] = 1
						tasksend = json.dumps(task)
						print("sending",task['task_id'],"to",CpyAllWorkers[0]['worker_id'])
						sock.sendall(tasksend.encode())
						sock.close()
						TaskWorker[task['task_id']] = CpyAllWorkers[0]['worker_id']
#						print("Task worker is updated",TaskWorker)
						updr = AllWorkers.index(CpyAllWorkers[0])
						AllWorkers[updr]['slots'] -= 1
#						print("Slots updated",AllWorkers)
					else:
						continue
				else:
					continue
	print("LL is done")

	
			
def analyse():
	count = 0.
#	print("In analuse")
	while True:

		for worker in AllWorkers:
			l = list(worker.values())
			Ana.write(str(count)+','+str(l[0])+','+str(l[1])+'\n')
			Ana.flush()
		count += 0.5
		time.sleep(0.5)


AllWorkers = []
TaskWorker = {}
if __name__ == '__main__':
	try:
		if(len(sys.argv)!=3):
			print("Usage: python master.py config.json Algo")
			exit()	
		TCT = open("TCT.txt","a")
		TCT.write(sys.argv[-1]+'\n')
		TCT.flush()
		JobArr = open("JA.txt","a")
		JobArr.write(sys.argv[-1]+'\n')
		JobArr.flush()
		Ana = open("Analyse.txt","a")
		Ana.write(sys.argv[-1]+'\n')
		Ana.flush()
		WS = open("WS.txt","w")
		f = open(sys.argv[1])
		r = json.load(f)
		for i in r['workers']:
			AllWorkers.append(i)
			WS.write(str(i["worker_id"])+","+str(i["slots"])+'\n')
			WS.flush()
	#	print(AllWorkers)
		NoOfWorkers = len(AllWorkers)
		RequestThread = threading.Thread(target=accept_jobs)
		RequestThread.start()
		ListenThread = threading.Thread(target = ListenToWorkers,args=(NoOfWorkers,))
		ListenThread.start()
		AnalyseThread = threading.Thread(target = analyse)
		if sys.argv[-1] == "Random":	
			RandomThread = threading.Thread(target=Random)
			RandomThread.start()
		elif sys.argv[-1] == "RR":
			RRThread = threading.Thread(target=RR)
			RRThread.start()
		else:
			LLThread = threading.Thread(target=LL)
			LLThread.start()
			
	except KeyboardInterrupt:
		print("okay byre")
		TCT.write("END")
