# This script is clean up amazon catalog data set
# It is picking up id, ASIN, title, categories (product belongs to many categories)
# From a given category path, it is picking up top level category, 3rd level category and leaf level category


def process(record):
  """
  A record is a product record, which is stored as multiline record 
  in imported Amazon catalog data. Only relevant fields are extracted.  
  """
  #print 'record found'
  pid = ''
  asin = ''
  title = ''
  categories = []
  for line in record:
    if line[:3] == 'Id:':
      pid = line[3:].strip()
    if line[:5] == 'ASIN:':
      asin = line[5:].strip()
    if line[:8] == '  title:':
      title = line[8:].strip().replace(',', '-')
    if line[:4] == '   |':
      cat_list = line.strip().replace(',', '-').split('|')
      # print cate
      if len(cat_list) > 3: 
        i = cat_list[1].find('[')
        cat1 = cat_list[1][:i]    # select 3rd level category
        
        i = cat_list[3].find('[')
        cat2 = cat_list[3][:i]    # select 3rd level category
        
        n = len(cat_list) - 1
        i = cat_list[n].find('[')
        cat3 = cat_list[n][:i]    # select leaf level category

        chosen =  cat1 + '|' + cat2 + '|' + cat3
        
        categories.append(chosen)
  # flatten the categories before writing
  log = ''
  for category in set(categories):  # make set to remove duplicate categories
    log = log + '%s,%s,%s\n' %(pid, title, category)
  return log


with open('amazon-meta.txt','r') as input_file:
  fout = open('nile-catalog.txt','a')
  record = []
  for line in input_file:
    if len(line.strip()) == 0:
        log = process(record)
        print log
        fout.seek(2,0)
        fout.write(log)
        fout.flush()
        record = []
    else:
      record.append(line)
fout.close()
