#!/usr/bin/env python

## generates sales data and pass it thru kafka producer
## log format
##   timestamp (in ms), customer_id, product_id, qty, cost, zipcode


from kafka.producer import SimpleProducer
from kafka import KafkaClient
import time
import random

zipcodes=['95054','94085','94040','94301','95036','95015','95051','94081','94041','95035']

def orderProducer():
  #client = KafkaClient('localhost:9092')
  client = KafkaClient('ip-172-31-3-237.us-west-1.compute.internal')
  producer = SimpleProducer(client)
  while True:
    timestamp = int(time.time() * 1000) # to make it milisecond
    customer_id = random.randint(1,100000)
    product_id = random.randint(190,210)
    qty = random.randint(1,3)
    zipcode = zipcodes[customer_id % 10]

    #cost is in cents, could be zero
    cost = random.randint(1,200) - 20
    if (cost < 0):
      cost = 0

    order = "%s,%s,%s,%s,%s,%s" % ( timestamp, customer_id, product_id, qty, cost, zipcode )
    print order
    producer.send_messages('order', order)

    time.sleep(1)

def main():
  orderProducer()

if __name__ == "__main__":
  main()




