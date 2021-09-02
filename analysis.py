import statistics as stat
import numpy as np
import matplotlib.pyplot as plt
tct=open("TCT.txt","r")

d={}
present_algo=""
for i in tct:
	
	if i.upper().strip() in ["RANDOM","RR","LL"]:

		d[i.upper().strip()]=[]
		present_algo=i.upper().strip()
	else:
		i=i.split(",")
		worker_no=int(i[0])
		task_id=i[1]
		time1=float(i[2])
		time2=float(i[3][0:-1])
		d[present_algo].append([worker_no,task_id,time1,time2])




task_mean=[]
task_median=[]
for i in d.keys():
	lis=[j[3]-j[2] for j in d[i] ]
	mean=stat.mean(lis)
	median=stat.median(lis)
	task_mean.append(mean)
	task_median.append(median)



# 


d2={}
ja=open("JA.txt","r")
present_algo=""
for i in ja:
	
	if i.upper().strip() in ["RANDOM","RR","LL"]:

		d2[i.upper().strip()]=[]
		present_algo=i.upper().strip()
	else:
		i=i.split(",")
		job_arrival_time=float(i[0])
		job_id=int(i[1][0:-1])
		d2[present_algo].append([job_id,job_arrival_time])

d3={}
for algo in d.keys():
	d3[algo]=[]
	for i in d2[algo]:

		job_id=i[0]
		k=len(str(job_id))
		start=i[1]
		end=0
		for j in d[algo]:
			if j[1][0:k]==str(job_id):
				if j[2]>end:
					end=j[2]


		d3[algo].append(end-start)




job_median=[]
job_mean=[]
for i in d3.keys():
	mean=stat.mean(d3[i])
	median=stat.median(d3[i])
	job_mean.append(mean)
	job_median.append(median)





n_groups =3

fig, ax = plt.subplots()
index = np.arange(n_groups)
bar_width = 0.18
opacity = 0.8


rects1 = plt.bar(index, task_mean, bar_width-0.01,
alpha=opacity,
color='b',
label='Task Mean')

rects2 = plt.bar(index + bar_width, task_median, bar_width-0.01,
alpha=opacity,
color='orange',
label='Task Median')

rects3 = plt.bar(index + 2*bar_width, job_mean, bar_width-0.01,
alpha=opacity,
color='g',
label='Job Mean')

rects4 = plt.bar(index +3* bar_width, job_median, bar_width-0.01,
alpha=opacity,
color='r',
label='Job Median')


plt.xlabel('ALGORITHM')
plt.ylabel('TIME')
plt.xticks(index + bar_width, ('RANDOM', 'RR', 'LL' ))

plt.legend()

plt.tight_layout()
plt.show()


no_of_worker=0

ws=open("WS.txt","r")

d5={}
for i in ws:
	no_of_worker+=1
	w_id,max_slot=i.split(",")
	d5[w_id.strip()]=int(max_slot[0:-1])



wo=open("Analyse.txt","r")

d3={}
present_algo=""
for i in wo:
	
	if i.upper().strip() in ["RANDOM","RR","RANSOM","LL"]:

		d3[i.upper().strip()]=[]
		present_algo=i.upper().strip()
	else:
		i=i.split(",")
		time=float(i[0])
		worker_no=int(i[1])
		slot_free=int(i[2][0:-1])
		d3[present_algo].append([time,worker_no,slot_free])






d4={}
for al in d3.keys():
	Y=[[] for i in range(no_of_worker)]
	x=[]
	for m in d3[al]:
		if m[0] not in x:
			x.append(m[0])
		Y[m[1]-1].append(d5[str(m[1]).strip()]-m[2])
	key=x[0]
	for i in range(len(x)):
		x[i]-=key
	for w in range(no_of_worker):
		plt.step(x,Y[w],label="worker "+str(w+1),marker="o")
	plt.xticks(np.arange(0, max(x), 0.5))
	plt.xticks(rotation=90)
	plt.xlabel('TIME')
	plt.ylabel('Running Task')


	

		
	plt.legend()
	plt.title(al)
	plt.show()

