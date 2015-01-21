#!/usr/bin/env python

## generates product data files
## log format
##   product_id, name, category1, category2, category3

## timestamp converstions testing site : http://www.epochconverter.com/


## ----- config
entries=10000000

import os
import datetime as dt
import random
import json

# overwrite this function to customize log generation
def generate_log(product_id):
  category1 = random.randint(1,10)
  category2 = random.randint(1,10)
  category3 = random.randint(1,10)

  #csv
  logline = "%s,%s,%s,%s" % (product_id, category1, category2, category3)

  #print logline
  return logline



#main
## --- script main
if __name__ == '__main__':
  
  filename = "product"  + ".log"
  with open(filename, "w") as fout:
    print "generating log ", filename
    for entry in range(0, entries):
      logline = generate_log(entry)
      fout.write(logline + "\n")
