#!/usr/bin/env python

## generates product data files
## log format
##   product_id, name, category1, category2, category3

## timestamp converstions testing site : http://www.epochconverter.com/


## ----- config
entries = 1000
categories = ['History','Spirituality','Fiction','Romance','Crafts & Hobbies','Home & Garden','Translations','Religion','Reference','Fantasy']

import os
import datetime as dt
import random
import json


# overwrite this function to customize log generation
def generate_log(product_id):
  name = "Book_%s" % (product_id)
  category1 = categories[random.randint(0,9)]
  category2 = categories[random.randint(0,9)]
  category3 = categories[random.randint(0,9)]

  #csv
  logline = "%s,%s,%s,%s,%s" % (product_id, name, category1, category2, category3)

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
