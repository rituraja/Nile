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
  group = ''
  categories = set()
  for line in record:
    if line[:3] == 'Id:':
      pid = line[3:].strip()
    if line[:5] == 'ASIN:':
      asin = line[5:].strip()
    if line[:8] == '  title:':
      title = line[8:].strip().replace(',', '-')
    if line[:8] == '  group:':
      group = line[8:].strip().replace(',', '-')
    
    if line.lstrip()[0] == '|':
      category = line.strip().replace(',', '-').split('|')
      cat_name_path = map(lambda item: item[:item.find('[')].strip(), category)
      parent = ''
      for item in cat_name_path:
        if item != '':
          if parent != '':
            parent = parent + '|' + item
          else:
            parent = item
          categories.add(parent)
  
  # print categories
  all_categories = '%'.join(sorted(list(categories)))
  log = '%s,%s,%s\n' %(pid, title, all_categories)
  
  return log


with open('amazon-meta.txt','r') as input_file:
  fout = open('nile-meta.txt','a')
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
