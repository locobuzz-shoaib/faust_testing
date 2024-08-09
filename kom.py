import json


def convert_to_python_dict(data_string):
    # Replace curly braces and equal signs with Python dictionary syntax
    data_string = data_string.replace('=', ':')

    # Convert the string to a Python dictionary
    python_dict = json.loads(data_string.replace("'", '"').replace("True", "true").replace("False", "false"))

    # Parse the FilterJson string
    python_dict['FilterJson'] = json.loads(python_dict['FilterJson'])

    return python_dict


data_string = "{ AlertSettingID = 5, FilterJson = \"{ \\\"FilterData\\\" : { \\\"FilteName\\\" : \\\"PriyanshuFilter\\\"}, \\\"FilterType\\\" : \\\"AlertFilter\\\" }\", Edited = true, StreamName = \"AlertStream_5_key_86858\" }"

python_dict = convert_to_python_dict(data_string)
print(python_dict)
