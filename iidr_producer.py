import os
import json
import sys
from kafka import KafkaProducer

valid_file = ["S_DOC_QUOTE.txt"]
f_name = " "

producer = KafkaProducer(bootstrap_servers='localhost:9092')

class iidr:

    def __init__(self,in_file_name):
        self.in_file_name = in_file_name

    def fetch_name(self):
        print(self.in_file_name)
        f_name = os.path.basename(self.in_file_name.replace(".txt", ""))
        print("table_key",f_name)
        return(f_name)

    def valid_file(self):
        if self.in_file_name in valid_file[0] :
            print("valid file")
            return(True)
        else:
            print("invalid file")
            return(False)

    def iidr_send_data(self):
        header = [("table.key", f_name.encode())]
        with open(self.in_file_name, "r") as f1:
            for msg in f1:
                print(msg)
                msg = msg.strip("\n")
                producer.send("test20", value=msg.encode(), headers=header)
                producer.flush()
            print("Data published successfully")


if len(sys.argv) > 0 :
# Extract the file name
    f_name_ext = iidr(sys.argv[1])
    f_name = f_name_ext.fetch_name()
# Validate the file name
    f_name_valid = iidr(f_name)
    status = f_name_valid.valid_file()

# Call the producer method if File is valid
    if status :
            send_data = iidr(sys.argv[1])
            send_data.iidr_send_data()
            print("Process Complete:")
    else:
            print("Invalid file is passed in argument")
else:
    print("Arguments or File path is not passed")



