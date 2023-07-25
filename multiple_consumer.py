import threading
import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-0ww79.australia-southeast2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '6LGTGYXCLT6MOOOR',
    'sasl.password': 'RRV0i94ilJOMQCS25Fj73C2D9h0ihhke3BbV3FTNt1lIaf/Uxcj5JBIrkD5SQy6A',
    'group.id': 'group_8',
    'auto.offset.reset': 'earliest'
}

# Function to apply data transformation logic to the Avro data
def transform_data(avro_data):
    # Update the category column to uppercase
    avro_data['category'] = avro_data['category'].upper()

    # Apply some business logic to update the price column based on the category
    if avro_data['category'] == 'ELECTRONICS':
        avro_data['price'] -= 10  # Apply a discount for special category products

    return avro_data


# Event to signal the threads to stop
stop_event = threading.Event()

def consume_messages(consumer):
    global stop_event

    # Subscribe to the 'product_updates' topic
    consumer.subscribe(['product_updates'])

    # Create a separate JSON file for each consumer
    file_name = f"consumer_{threading.current_thread().name}.json"
    with open(file_name, 'a') as json_file:
        # Continually read messages from Kafka
        while not stop_event.is_set():
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue

            # Deserialize the Avro data into a Python object
            avro_data = msg.value()

            # Apply data transformation logic
            transformed_data = transform_data(avro_data)

            # Convert the transformed data to a JSON string
            json_str = json.dumps(transformed_data)

            # Append the JSON string to the file with a new line
            json_file.write(json_str + '\n')

    # Close the consumer when the event is set
    consumer.close()

# Number of consumers in the group
num_consumers = 5

# Create a list to hold the consumer instances
consumers = []

# Create 5 consumer instances
for i in range(num_consumers):
    schema_registry_client = SchemaRegistryClient({
        'url': 'https://psrc-10wgj.ap-southeast-2.aws.confluent.cloud',
        'basic.auth.user.info': '{}:{}'.format('VC5JAR4EETZAASKD', 'JEZ5p+uf7bi6P3vCOOOnt8jcLT6IzLSb18zS8ABxg59gY+FU6+eCpoxFJGoeMJsI')
    })

    # Fetch the latest Avro schema for the value
    subject_name = 'product_updates-value'
    schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

    # Create Avro Deserializer for the value
    key_deserializer = StringDeserializer('utf_8')
    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    # Define the DeserializingConsumer
    consumer = DeserializingConsumer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.deserializer': key_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': kafka_config['group.id'],
        'auto.offset.reset': kafka_config['auto.offset.reset'],
    })

    consumers.append(consumer)

# Start consuming messages for each consumer in a separate thread
try:
    consumer_threads = []
    for consumer in consumers:
        thread = threading.Thread(target=consume_messages, args=(consumer,))
        consumer_threads.append(thread)
        thread.start()

    # Wait for KeyboardInterrupt to stop the threads
    while True:
        try:
            # Wait for the threads to finish, with a timeout of 1 second
            for thread in consumer_threads:
                thread.join(1.0)

            # Check if all threads have finished
            if all(not thread.is_alive() for thread in consumer_threads):
                break

        except KeyboardInterrupt:
            print("Keyboard interrupt detected. Signaling consumers to stop.")
            stop_event.set()
            break

    print("All consumers stopped.")

except Exception as e:
    print("An error occurred: ", e)
finally:
    print("Exiting program.")