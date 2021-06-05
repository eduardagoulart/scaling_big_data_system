def transform_data(data):
    if type(data) == int:
        return data + 1
    elif type(data) == str:
        return chr((ord(data.upper()) + 1 - 65) % 26 + 65)
    elif data is True:
        return False
    return True


print(list(map(transform_data, [1, "a", True])))
print(list(map(transform_data, [5, "d", False])))
