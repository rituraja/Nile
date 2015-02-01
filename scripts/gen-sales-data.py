#!/usr/bin/env python

## generates sales data files
## log format
##   timestamp (in ms), customer_id, product_id, qty, cost, zipcode

## timestamp converstions testing site : http://www.epochconverter.com/


## ----- config
days=29
entries_per_day=10000
zipcodes=['95054','94085','94040','94301','95036','95015','95051','94081','94041','95035']

import os
import datetime as dt
import random
import json

# overwrite this function to customize log generation
def generate_log(timestamp):
  customer_id = random.randint(1,100000)
  product_id = random.randint(1,542684)
  name = product_id # need to remove later
  qty = random.randint(0,5)
  zipcode = zipcodes[customer_id % 10]

  #cost is in cents, could be zero
  cost = random.randint(1,200) - 20
  if (cost < 0):
    cost = 0

  #csv
  logline = "%s,%s,%s,%s,%s,%s,%s\n" % (timestamp, customer_id, product_id,name, qty, cost, zipcode)

  #print logline
  return logline



#main
## --- script main
if __name__ == '__main__':
  time_inc_ms = int ((24.0*3600*1000)/entries_per_day)
  #print "time inc ms", time_inc_ms
  #epoch = dt.datetime.fromtimestamp(0)
  epoch = dt.datetime(1970,1,1)

  year_start = dt.datetime(2014, 12, 1)
  for day in range(0, days):
    day_delta = dt.timedelta(days=day)
    start_ts = year_start + day_delta
    #end_ts = dt.datetime(start_ts.year, start_ts.month, start_ts.day, 23, 59, 59)
    end_ts = dt.datetime(start_ts.year, start_ts.month, start_ts.day+1, 0, 0, 0)
    filename = "sales-" + start_ts.strftime("%Y-%m-%d") + ".log"
    #print start_ts
    #print end_ts
    last_ts = start_ts

    with open(filename, "w") as fout:
      print "generating log ", filename
      while (last_ts < end_ts):
        delta_since_epoch = last_ts - epoch
        millis = int((delta_since_epoch.microseconds + (delta_since_epoch.seconds + delta_since_epoch.days * 24 * 3600) * 10**6) / 1e3)
        #print "last ts", last_ts
        #print "millis",  millis
        logline = generate_log(millis)
        fout.write(logline)

        last_ts = last_ts + dt.timedelta(milliseconds=time_inc_ms)

