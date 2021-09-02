* Implemented containerised master-worker architecture in Python to simulate scheduling algorithms in a distributed environment. 

* Implemented scheduling algorithms to schedule jobs across multiple workers considering resource requirements.

* **Concepts used:**__ Socket Programming , Scheduling algorithms , multithreading in Python.

**--->FILE DESCRIPTION**:

  request.py -> Sends JOB requests to the master on PORT 5000.

  master.py -> Listen for JOB requests on PORT 5000 & schedules the jobs to workers on PORT 5001.

  worker.py -> Send ACK to Master on PORT 5001.

  analysis.py -> Code For Graphical Performance comparison between the scheduling algorithms.

  config.json -> Contains Worker Details(worker_id,slots,port).
