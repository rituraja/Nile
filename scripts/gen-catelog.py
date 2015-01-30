#!/usr/bin/env python

## generates product data files
## log format
##   product_id, name, category1, category2, category3

## timestamp converstions testing site : http://www.epochconverter.com/


## ----- config
entries = 1000
categories = ['History','Spirituality','Fiction','Romance','Crafts & Hobbies',
'Home & Garden','Translations','Religion','Reference','Fantasy',
 'Music','NonFiction','Food','Jazz', 'Arts',
 'Crime','Science','Photography','Essay','Sports']

import os
import datetime as dt
import random
import json


# overwrite this function to customize log generation
def generate_log(i, prod_name):
  product_id = i
  category1 = categories[random.randint(0,19)]

  #csv
  logline = "%s,%s,%s" % (product_id, prod_name, category1)

  #print logline
  return logline



#main
## --- script main
if __name__ == '__main__':
  with open('titles.txt','r') as titles:
		fout = open("product.txt", "a")
		print "generating log "
		i = 1000
		for title in titles:
			i = i+1
  		logline = generate_log(i,title[:-1])
  		fout.seek(2,0)
  		fout.write(logline + "\n")
  		fout.flush()
  fout.close()
