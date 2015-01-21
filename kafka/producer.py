from kafka.producer import SimpleProducer
from kafka import KafkaClient

def producer():
  client = KafkaClient('localhost:9092')
  producer = SimpleProducer(client)
  with open('../scripts/product.log') as products:
    for product in products:
      producer.send_messages('product', product)

def main():
  producer()

if __name__ == "__main__":
  main()



        