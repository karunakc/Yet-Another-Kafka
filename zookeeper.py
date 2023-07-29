import subprocess
import time
import shutil

producers = sys.argv[1]
consumers =sys.argv[2]

broker2=subprocess.Popen(["python", "broker2.py"])
broker3=subprocess.Popen(["python", "broker3.py"])
broker1=subprocess.Popen(["python", "broker1.py",producers,consumers])

while True:
    time.sleep(5)
    shutil.copyfile('broker1/log.log','broker2/broker1-log.log')
    shutil.copyfile('broker1/log.log','broker3/broker1-log.log')
    if broker1.poll()==None:
        print("Broker 1 Alive")

    else:
        print("Broker 1 RIP")
    time.sleep(5)
    if broker2.poll()==None:
        print("Broker 2 Alive")
    else:
        print("Broker 2 RIP")
    time.sleep(5)
    if broker2.poll()==None:
        print("Broker 3 Alive")
    else:
        print("Broker 3 RIP")
    time.sleep(20)
