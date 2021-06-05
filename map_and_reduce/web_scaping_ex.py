input_data = [
    {
        "headers": ("01/19/2018", "Mozilla", 300),
        "response": {
            "text": "Hello world!",
            "encoding": "utf-8",
        },
    },
    {
        "headers": ("01/19/2018", "Chrome", 404),
        "response": {
            "text": "Page not found",
            "encoding": "ascii",
        },
    },
    {
        "headers": ("01/19/2018", "Mozilla", 300),
        "response": {
            "text": "Yet another page",
            "encoding": "utf-8",
        },
    },
]


def get_response_text(data):
    return data["response"]["text"]


print(list(map(get_response_text, input_data)))
