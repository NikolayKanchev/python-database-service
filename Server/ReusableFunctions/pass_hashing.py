def hash_password(password):
    hashed_pass = ""
    counter = 0
    salt = "The best hashing ever !!!"

    for symbol in password:
        hashed_pass = hashed_pass + chr(ord(symbol) + ord(str(len(password))) + 1122)
        hashed_pass = hashed_pass + salt[counter]
        counter = counter + 1
    return hashed_pass
