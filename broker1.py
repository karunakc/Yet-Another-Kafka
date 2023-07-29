import socket
from _thread import *
import os
from datetime import datetime
import logging
import sys

ThreadCount = 0
topics_list={}
consumer_time={}
producers = int(sys.argv[1])
consumers = int(sys.argv[2])

def file_path_creations(topic_name):
    if not os.path.exists(f"broker1/"+topic_name):
        os.makedirs(f"broker1/"+topic_name)
        filename=f"broker1/{topic_name}/{topic_name}_partition1.txt"
        f = open(filename, "w")
        f.close()
        filename=f"broker1/{topic_name}/{topic_name}_replication2.txt"
        f = open(filename, "w")
        f.close()
        filename=f"broker1/{topic_name}/{topic_name}_replication3.txt"
        f = open(filename, "w")
        f.close()
    if not os.path.exists(f"broker2/"+topic_name):
        os.makedirs(f"broker2/"+topic_name)
        filename=f"broker2/{topic_name}/{topic_name}_partition2.txt"
        f = open(filename, "w")
        f.close()
        filename=f"broker2/{topic_name}/{topic_name}_replication3.txt"
        f = open(filename, "w")
        f.close()
        filename=f"broker2/{topic_name}/{topic_name}_replication1.txt"
        f = open(filename, "w")
        f.close()
    if not os.path.exists(f"broker3/"+topic_name):
        os.makedirs(f"broker3/"+topic_name)
        filename=f"broker3/{topic_name}/{topic_name}_partition3.txt"
        f = open(filename, "w")
        f.close()
        filename=f"broker3/{topic_name}/{topic_name}_replication2.txt"
        f = open(filename, "w")
        f.close()
        filename=f"broker3/{topic_name}/{topic_name}_replication1.txt"
        f = open(filename, "w")
        f.close()



def producer_handler(connection, address):


    while True:
        topic_name=connection.recv(2048).decode('utf-8')

        if topic_name=='0':
            logging.warning(f"{address} closed connection")
            print(f"{address} closed connection")
            break
        else:
            if topic_name not in topics_list:
                topics_list[topic_name]=[]
                topics_list[topic_name].append(address)
                logging.info(f'{address} is added to {topic_name} list')
                logging.info('create all filepaths necessary for the topic in all brokers')

                file_path_creations(topic_name)
                
            else:
                if address not in topics_list[topic_name]:
                    topics_list[topic_name].append(address)
                    logging.info(f'{address} is added to {topic_name} list')
            print("registered publishers for the topic: ", topics_list[topic_name])

            filename1=f"broker1/{topic_name}/{topic_name}_partition1.txt"
            filename2=f"broker2/{topic_name}/{topic_name}_replication1.txt"
            filename3=f"broker3/{topic_name}/{topic_name}_replication1.txt"

            broker2Socket.send(str.encode(topic_name))
            logging.info(f"broker1 sent {topic_name} to broker2")
            print(topic_name)
            broker3Socket.send(str.encode(topic_name))
            logging.info(f"broker1 sent {topic_name}to broker3")
            print(topic_name)


            while True:
                

                msg=connection.recv(2048).decode('utf-8')
                logging.info(f'{address} sent {msg} to {topic_name} broker1')
                msg=msg.strip().split(":")

                if msg[1]=='eof':
                    broker2Socket.send(str.encode('eof'))
                    broker3Socket.send(str.encode('eof'))
                    break
                
                if msg:
                    connection.send(str.encode('success'))
                else:
                    connection.send(str.encode('fail'))

                

                if int(msg[0])%3==1:
                    f1 = open(filename1, "a")
                    f2 = open(filename2, "a")
                    f3 = open(filename3, "a")
                    f1.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ "\t"+msg[1]+"\n")
                    logging.info(f"broker1 wrote {msg[1]} to broker1/partition1")
                    f1.close()
                    f2.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ "\t"+msg[1]+"\n")
                    logging.info(f"broker1 wrote {msg[1]} to broker2/replication1")
                    f2.close()
                    f3.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ "\t"+msg[1]+"\n")
                    logging.info(f"broker1 wrote {msg[1]} to broker3/replication1")
                    f3.close()
                    
                elif int(msg[0])%3==2:
                    broker2Socket.send(str.encode(msg[1]))
                    logging.info(f"broker1 sent {msg[1]} to broker2")
                elif int(msg[0])%3==0:
                    broker3Socket.send(str.encode(msg[1]))
                    logging.info(f"broker1 sent {msg[1]} to broker3")
                
                
                

    connection.close()



def consumer_handler(connection, address):

    while True:
        topic_action=connection.recv(2048).decode('utf-8')
        logging.info(f'{address} requested for {topic_action}')
        if topic_action=='0':
            print(f"{address} closed the connection")
            break
        
        topic,action,reg_time=topic_action.split(',')

        if address in consumer_time:
            reg_time=consumer_time[address]
        else:
            consumer_time[address]=reg_time

            
        lst = os.listdir("broker1")
        print(lst)
        if topic in lst:
            filename1=f"broker1/{topic}/{topic}_partition1.txt"
            filename2=f"broker1/{topic}/{topic}_replication2.txt"
            filename3=f"broker1/{topic}/{topic}_replication3.txt"
            f=open(filename1,'r')
            lines=f.readlines()
            f.close()
            f=open(filename2,'r')
            lines+=f.readlines()
            f.close()
            f=open(filename3,'r')
            lines+=f.readlines()
            f.close()
            lines.sort()
            print(lines)

            if action=='read --from-beginning':
                for i in lines:
                    msg=i.strip().split('\t')
                    msg=msg[1]
                    connection.send(str.encode(msg))
                    logging.info(f"broker1 sent {msg} to consumer")
                    logging.info(f'{msg} sent to {address} for {topic}')
                    ack=connection.recv(2048).decode('utf-8')
                    if ack!='ack':
                        connection.send(str.encode(msg))
                        logging.info(f"broker1 sent ack to consumer")
                connection.send(str.encode('eof'))
                logging.info(f"broker1 sent eof to consumer")
            
            elif action=='read':
                for i in lines:
                    msg=i.strip().split('\t')
                    if msg[0]>reg_time:
                        msg=msg[1]
                        connection.send(str.encode(msg))
                        logging.info(f"broker1 sent {msg} to consumer")
                        ack=connection.recv(2048).decode('utf-8')
                        if ack!='ack':
                            connection.send(str.encode(msg))
                            logging.info(f"broker1 sent ack to consumer")
                connection.send(str.encode('eof'))
                logging.info(f"broker1 sent eof to consumer")

        
            f.close()
            
        else:
            connection.send(str.encode('Topic doesnt exist, creating'))
            logging.info(f"broker1 no topic creating to consumer")
            os.makedirs("broker1/"+topic)
            filename=f"broker1/{topic}/{topic}.txt"
            f = open(filename, "w")
            f.close()
            topics_list[topic]=[]
            topics_list[topic].append(address)

    connection.close()


def start_server():
    
    host=socket.gethostname()

    #producer socket
    producer_socket = socket.socket()
    producer_socket.bind((host, 9999))
    producer_socket.listen(producers)

    #consumer socket
    consumer_socket=socket.socket()
    consumer_socket.bind((host,9998))
    consumer_socket.listen(consumers)




    while True:
        
        
        producer, p_address = producer_socket.accept()
        print('Connected to producer: ' + p_address[0] + ':' + str(p_address[1]))
        start_new_thread(producer_handler, (producer, p_address, ))
        
        consumer, c_address = consumer_socket.accept()
        print('Connected to consumer: ' + c_address[0] + ':' + str(c_address[1]))
        start_new_thread(consumer_handler, (consumer, c_address, ))





# broker 2 socket
broker2Socket = socket.socket()
print('Producer waiting for the broker')
broker2Socket.connect((socket.gethostname(), 9993))

# broker 3 socket
broker3Socket = socket.socket()
print('Producer waiting for the broker')
broker3Socket.connect((socket.gethostname(), 9992))

if not os.path.exists("broker1"):
    os.makedirs("broker1")
logging.basicConfig(filename='broker1/log.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')
start_server()

''' 
        
if __name__ =="__main__":
    logging.basicConfig(filename='broker.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(message)s')
    a=threading.Thread(target=start_server)
    b=threading.Thread(target=zookeeper)
'''

