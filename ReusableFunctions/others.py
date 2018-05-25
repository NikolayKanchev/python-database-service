def remove_special_char(sql_string):

    str_to_return = sql_string.replace('"', '')

    str_to_return = str_to_return.replace('\\', '')

    return str_to_return
