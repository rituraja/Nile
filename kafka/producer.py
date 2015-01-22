from kafka.producer import SimpleProducer
from kafka import KafkaClient

def producer():
  client = KafkaClient('localhost:9092')
  producer = SimpleProducer(client)
  with open('/home/ubuntu/Nile/data/product.log') as products:
    for product in products:
      print product
      producer.send_messages('product', product)

def main():
  producer()

if __name__ == "__main__":
  main()



        
