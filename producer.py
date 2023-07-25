import mysql.connector
import pandas as pd
from datetime import datetime


from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
 
# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-0ww79.australia-southeast2.gcp.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '6LGTGYXCLT6MOOOR',
    'sasl.password': 'RRV0i94ilJOMQCS25Fj73C2D9h0ihhke3BbV3FTNt1lIaf/Uxcj5JBIrkD5SQy6A'
}   

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-10wgj.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('VC5JAR4EETZAASKD', 'JEZ5p+uf7bi6P3vCOOOnt8jcLT6IzLSb18zS8ABxg59gY+FU6+eCpoxFJGoeMJsI')
})

# Fetch the latest Avro schema for the value
subject_name = 'product_updates-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print(schema_str)

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})

# Fetch incremental data from MySQL
# To establish sql connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Simba@123",
    database="db2"
)

mycursor = mydb.cursor()

mycursor.execute("SELECT last_read_timestamp FROM last_read")

myresult = mycursor.fetchone()

if myresult:
    last_read_timestamp = myresult[0]
else:
    # Set the timestamp if not available
    last_read_timestamp=datetime(2023, 7, 9, 12, 30)
    
    #Insert the last_read_timestamp into database
    insert_query = "INSERT INTO last_read (last_read_timestamp) VALUES (%s)"
    mycursor.execute(insert_query, (last_read_timestamp,))
    mydb.commit() 
    
#sql query to fetch all records where last_updated > last_read_timestamp
query = "SELECT * FROM product WHERE last_updated > %s"   
mycursor.execute(query, (last_read_timestamp,))

#Fetch all records
records = mycursor.fetchall()
    
# Load data from MySQL to Pandas dataframe
df = pd.DataFrame(records, columns=['id', 'name', 'category', 'price', 'last_updated'])
print(df.head())

# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    # print(value)
    # Convert the last_updated timestamp to string representation
    value['last_updated']=str(value['last_updated'])
    # Produce to Kafka
    producer.produce(topic='product_updates', key=str(row['id']), value=value, on_delivery=delivery_report)
    producer.flush()
    
print("Data successfully published to Kafka")

#Update the last_read_timestamp based on last record last_updated timestamp
if records:
    last_read_timestamp = records[-1][4]
else:
    print("No new records to process.")

# Update the last_read_timestamp in database
update_query = "UPDATE last_read SET last_read_timestamp = %s"
mycursor.execute(update_query, (last_read_timestamp,))
mydb.commit()

#Close the cursor and db connection
mycursor.close()
mydb.close()