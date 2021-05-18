from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #The following web page was very helpful
    #https://knowledge.udacity.com/questions/338547
    
    def generate_data(self):
        with open(self.input_file) as f:
            
            data = json.load(f)
            
            for line in data:
                message = self.dict_to_binary(line)
                
                # Sending the correct data -- completed                 
                print(f"Sending message {message}")                
                self.send(self.topic, message)                
                time.sleep(10)

    # TODO fill this in to return the json dictionary to binary --completed 
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
        