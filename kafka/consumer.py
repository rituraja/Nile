from kafka.consumer import SimpleConsumer
from kafka import KafkaClient

temp_file_path = "/tmp/kafka/products/"
hdfs_file_path = "/products/"
filename = "product.out"


def product_consumer():
  client = KafkaClient('localhost:9092')
  consumer = SimpleConsumer(client, 'product-group', 'product')
  while True:
    lines = consumer.get_messages(count=1000,block=False)
    for line in lines:
      print(line)
      dump(line.message.value)

def dump(product):
  global currFile
  currFile = open(temp_file_path + filename, "w") 
  isFull = False
  isFull = writeTempFile(product)
  if isFull == True:
    flush_to_hdfs(filename)

def flush_to_hdfs(tempfile):
  tempfile.close()
  os.system('/usr/bin/hdfs dfs -put -f %s %s' % (tempfile_path,hdfs_file))

def writeTempFile(product):
  currFile.write(product)
  if currFile.tell() > 10000000:
    return True
  else:
    return False


def main():
  product_consumer()

if __name__ == "__main__":
  main()



        
