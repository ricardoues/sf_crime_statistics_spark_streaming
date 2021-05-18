import producer_server


def run_kafka_server():
	# setting the variables. 
    input_file = "/home/workspace/police-department-calls-for-service.json"
    topic = "org.sanfranciscopolice.crime.statistics"
    server = "localhost:9092"
    client_id = "kafka-python-producer"

    
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=topic,
        bootstrap_servers=server,
        client_id=client_id
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
