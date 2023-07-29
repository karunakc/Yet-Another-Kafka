import socket

def producer():
    host = socket.gethostname()
    port = 9999

    producerSocket = socket.socket()
    print('Producer waiting for the broker')
    producerSocket.connect((host, port))

    while True:
        
        topic_name= input('Enter the topic name or 0 to exit: ')
        producerSocket.send(str.encode(topic_name))


        if topic_name!='0':
            id=1
            msg= input('\nEnter message: ')
            while True:
                producerSocket.send(str.encode(str(id)+":"+msg))
                id+=1
                if msg=='eof':
                    break
                ack = producerSocket.recv(2048).decode('utf-8')
                if ack=='fail':
                    print("resending the message")
                    producerSocket.send(str.encode(msg))
                else:
                    print("message sent successfully")
                msg= input('Enter message: ')
        else:
            break

    producerSocket.close()

producer()