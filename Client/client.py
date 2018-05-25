import threading
from socket import *

HOST = "192.168.56.1"
PORT = 8000

# region establish the connection
try:
    s = socket()
    s.connect((HOST, PORT))
except error:
    print("Connection to the server failed !!!" + str(error))
# endregion


# region gets the user input and sends it to the server
while True:
    user_input = input("Choose an option:\n"
                        "1) Menu\n"
                        "2) Write SQL\n")
    if user_input == '1' or user_input == '2':
        s.send(str.encode(user_input))
        break
# endregion


# region function for the new thread, which only listens for server response
def new_thread():
    server_reply = str(s.recv(1024), "UTF-8")
    print(f"< MyDB > { server_reply }")

    while server_reply != "":
        server_reply = str(s.recv(1024), "UTF-8")
        print(f"< MyDB > { server_reply }")


t1 = threading.Thread(target=new_thread)
t1.start()
# endregion


# region only sends the user input to the server (Working on the main thread)
while True:
    user_input = input()
    if user_input == "":
        continue

    s.send(str.encode(user_input))

    if user_input == "quit":
        exit()
# endregion
