from socket import *
from _thread import *

HOST = "192.168.56.1"
PORT = 8000

try:
    s = socket()
    s.connect((HOST, PORT))
except error:
    print("Connection to the server failed !!!" + str(error))

while True:
    user_input = input("Choose an option:\n"
                        "1) Menu\n"
                        "2) Write SQL\n")
    if user_input == '1' or user_input == '2':
        s.send(str.encode(user_input))
        break


def new_thread():
        server_reply = str(s.recv(1024), "UTF-8")
        print("< MyDB", server_reply)

        while server_reply != "":
            server_reply = str(s.recv(1024), "UTF-8")
            print("< MyDB", server_reply)


start_new_thread(new_thread, ())

while True:
    user_input = input()
    if user_input == "":
        continue

    s.send(str.encode(user_input))

    if user_input == "quit":
        exit()
