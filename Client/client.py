import threading
from socket import *
from texttable import Texttable

# HOST = "192.168.56.1"
HOST = "localhost"
PORT = 8000

# region establish the connection
s = socket()

try:
    s.connect((HOST, PORT))

except ConnectionRefusedError:

    print("Connection refused!!!")
# endregion

else:

    while True:

        user_input = input("Type 'sql' for SQL-mode and 'menu' for Menu-mode \n").replace(" ", "")

        if user_input.replace(" ", "") == "sql" or user_input == "menu":

            break

    s.send(str.encode(f'{user_input}'))
    # s.send(str.encode('sql'))

    # region New thread, which only listens for server response
    def new_thread():

        while True:

            server_reply = str(s.recv(1024), "UTF-8")

            if server_reply == "":

                break

            arrays = []

            if server_reply[0] == "[" and server_reply[-1] == "]":

                server_reply = server_reply[2:-2]

                arr = server_reply.replace("'", "").split("], [")

                for element in arr:

                    row_arr = element.split(", ")

                    arrays.append(row_arr)

            if len(arrays) != 0:

                t = Texttable()

                t.add_rows(arrays)

                print(t.draw())

            else:

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

    # endregion

