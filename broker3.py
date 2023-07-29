import socket
from _thread import *
import os
from datetime import datetime
import logging




def broker_handler():

    #broker 1 socket
    broker1socket=socket.socket()
    broker1socket.bind((socket.gethostname(),9992))
    broker1socket.listen(1)
    connection, address = broker1socket.accept()
    print("connected to broker 1")
    
  
    while True:
        topic_name=connection.recv(2048).decode('utf-8')
        logging.info(f"broker3 recv {topic_name} from broker1")

        filename1=f"broker3/{topic_name}/{topic_name}_partition3.txt"
        filename2=f"broker1/{topic_name}/{topic_name}_replication3.txt"
        filename3=f"broker2/{topic_name}/{topic_name}_replication3.txt"


        if topic_name=='0':
            logging.warning(f"{address} closed connection")
            print(f"{address} closed connection")
            break
            
        

        else:           

            while True:
                msg=connection.recv(2048).decode('utf-8')
                logging.info(f'broker1 sent {msg} to broker3')

                if msg=='eof':
                    break
                if msg==topic_name:
                    continue
                if msg:
                    connection.send(str.encode('success'))
                else:
                    connection.send(str.encode('fail'))
                f1 = open(filename1, "a")
                f2 = open(filename2, "a")
                f3 = open(filename3, "a")
                f1.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ "\t"+msg+"\n")
                logging.info(f'broker3 wrote {msg} to broker3/partition3')
                f1.close()
                f2.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ "\t"+msg+"\n")
                logging.info(f'broker3 wrote {msg} to broker1/replication3')
                f2.close()
                f3.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ "\t"+msg+"\n")
                logging.info(f'broker2 wrote {msg} to broker1/replication3')
                f3.close()

    connection.close()



if not os.path.exists("broker3"):
    os.makedirs("broker3")
logging.basicConfig(filename='broker3/log.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')
broker_handler()


