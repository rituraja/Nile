from kafka.consumer import SimpleConsumer
from kafka import KafkaClient

def product_consumer():
  client = KafkaClient('localhost:9092')
  consumer = SimpleConsumer(client, 'test-group', 'product')
  for product in consumer:
      print(product)

def main():
  product_consumer()

if __name__ == "__main__":
  main()



        