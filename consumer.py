import socket
from datetime import datetime 
import logging

def consumer():
    host = socket.gethostname()
    port = 9998

    consumerSocket = socket.socket()
    print('Waiting for broker to connect to')
    consumerSocket.connect((host, port))



    while True:
        topic_name=input("Enter topic name or 0 to exit: ")
        logging.info(f"{topic_name} requested by consumer")
        
        if topic_name=='0':
            consumerSocket.send(str.encode(topic_name))
            break
      
        todo = input('read type: ')
        logging.info(f"{todo} action requested by consumer")
        
        if topic_name in topic_time:
            reg_time=topic_time[topic_name]
        else:
            topic_time[topic_name]=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            reg_time=topic_time[topic_name]
       
        msg=f"{topic_name},{todo},{reg_time}"
        logging.info(f"{msg} sent by consumer to broker")

        consumerSocket.send(str.encode(msg))
        
        while True:
            topic_data = consumerSocket.recv(2048).decode()
            logging.info(f"{topic_data} received by consumer")
            
            if topic_data=='eof':
                break

            if topic_data=='Topic doesnt exist, creating':
                print(topic_data)
                break
            print(topic_data)
            consumerSocket.send(str.encode('ack'))
            

    consumerSocket.close()


topic_time={}
logging.basicConfig(filename='consumer.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')
consumer()