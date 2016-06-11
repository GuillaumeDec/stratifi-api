import sys
import requests
from lxml import etree
import re
import pandas

class cik:
   def __init__(self, cik, df):
      self.cc = ['company_name','cik','position','cusip','value','percent','tot_value','tot_entries']
      self.df = df
      self.cik = str(cik)
      self.url = "http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik_arg}&type=13F-HR&dateb=&owner=exclude&count=40&output=atom".format(cik_arg=self.cik)  
		# Uncomment to validate CIK:
		# The etree.parse() method fetches atom feed from the URL constructed above.
		# This atom feed contains a list of all 13F-HR filings for the given CIK.
      self.parsed = etree.parse(self.url)
      self.txt_link = ""
      self.primary_doc = ""
      self.info_table = ""
      
   def validate(self):
   	# Validate given ticker by making a call to EDGAR website with constructed URL.
   	cik_validation = requests.get(self.url)
   	if not '<?xml' in cik_validation.content[:10]:
   		return False
   	else:
   		return True
    
   def find_first_txt_link(self):
      # Finds index link of the first filing and from that link, constructs the link
      # to the full txt submission and stores it in self.txt_link
      
      entry_tag = "{http://www.w3.org/2005/Atom}entry"
#       print('go FIND ')
      
      link_tag = "{http://www.w3.org/2005/Atom}link"
      find_string = "{ent}/{link}".format(ent=entry_tag, link=link_tag)
#       print('GOO FIND')
      link = self.parsed.find(find_string).get("href")
      # Replaces "-index.htm" with ".txt"
      link_edit_index = link.find("-index.htm")
      self.txt_link = ''.join([link[:link_edit_index], ".txt"])
         
   def split_txt_file(self):
#       print('go split')
      txt_file = requests.get(self.txt_link).content
      iter_open_xml = re.finditer(r"<XML>", txt_file)
      iter_close_xml = re.finditer(r"</XML>",txt_file)
      
      opening_indices = [index.start()+len("<XML>\n") for index in iter_open_xml]
      closing_indices = [index.start() for index in iter_close_xml]
      
      self.primary_doc = txt_file[opening_indices[0]:closing_indices[0]]
      self.info_table = txt_file[opening_indices[1]:closing_indices[1]]
#       print('done split')
    
    # Uncomment the following if you want to see xml written to files:
    #
    # f_prim_doc = open("primary_doc.xml","w")
    # f_prim_doc.write(self.primary_doc)
    # f_prim_doc.close()
    # f_info_table = open("info_table.xml","w")
    # f_info_table.write(self.info_table)
    # f_info_table.close()
    
   def prep_csv_string(self, commaList):
   	
   	commaSeparated = []
   	for x in commaList:
   		if x is not None:
   			commaSeparated.append(x.text)
   		else:
   			commaSeparated.append('Empty')
   	commaSeparated = ",".join(commaSeparated)
   	return commaSeparated
   
   def write_primary_doc_csv(self):
      # Although the spec lists a lot of information for primary_doc, a lot
      # of it is things like addresses and agent names. So I'm ignoring everything except
      # periodOfReport, tableEntryTotal and tableValueTotal
      primary_doc_parse = etree.fromstring(self.primary_doc)
#       print('go WRITE')
      
      namespace = "{http://www.sec.gov/edgar/thirteenffiler}"
      headerData_tag = "".join([namespace, "headerData/"])
      filerInfo_tag = "".join([namespace, "filerInfo/"])
      formData_tag = "".join([namespace, "formData/"])
      summaryPage_tag = "".join([namespace, "summaryPage/"])  
      coverPage_tag = "".join([namespace, "coverPage/"])  
      filingManager_tag = "".join([namespace, "filingManager/"])  
      periodOfReport_tag = "".join([headerData_tag, filerInfo_tag, namespace, "periodOfReport"])
      filingManager_tag = "".join([formData_tag, coverPage_tag, filingManager_tag, namespace, "name"])
      tableEntryTotal_tag = "".join([formData_tag, summaryPage_tag, namespace, "tableEntryTotal"])
      tableValueTotal_tag = "".join([formData_tag, summaryPage_tag, namespace, "tableValueTotal"])
#       print('DOne write')
      
   	
   #    	commaList = []
   #    	commaList.append(primary_doc_parse.find(periodOfReport_tag))
   	#commaList.append(primary_doc_parse.find(tableEntryTotal_tag))
      self.comp_name = primary_doc_parse.find(filingManager_tag).text
      self.tot_entries = primary_doc_parse.find(tableEntryTotal_tag).text
      self.tot_val = primary_doc_parse.find(tableValueTotal_tag).text
#       print('exit write !!')
      
      return
    
   def write_info_table_csv(self):
      # I'm getting all the information in each infoTable tag and writing to a csv file
      
#       print('go Write INFO ')
      try:
         info_table_parse = etree.fromstring(self.info_table)
      except:
         return
#       print('go Write INFO 1111')
            
      namespace = "{http://www.sec.gov/edgar/document/thirteenf/informationtable}"
      infoTable_tag = "".join([namespace, "infoTable"])
      infoTable_tag2 = infoTable_tag + "/"
      nameOfIssuer_tag = 	"".join([namespace, "nameOfIssuer"])
      titleOfClass_tag = "".join([namespace, "titleOfClass"])
      cusip_tag = "".join([namespace, "cusip"])
      value_tag = "".join([namespace, "value"])
      list_poz = []
      list_cusip = []
      list_val = []
      list_percent = []
#       print('go Write INFO 2')
      for info_table in info_table_parse.findall(infoTable_tag):
         list_poz.append(info_table.find(nameOfIssuer_tag).text)
         list_cusip.append(info_table.find(cusip_tag).text)
         list_val.append(info_table.find(value_tag).text)
         ############## FINISH THIS
         try:
            list_percent.append( round( 100 * float(list_val[-1])/float(self.tot_val), 2 ) )
         except:
            list_percent.append(0.)
      
#       print('append= ',[self.comp_name,self.cik,list_poz,list_cusip,list_val,self.tot_val,self.tot_entries])
      tmp = pandas.DataFrame([[self.comp_name,self.cik,list_poz,list_cusip,list_val,list_percent,self.tot_val,self.tot_entries]], columns = self.cc)
#       print('TMP=== ', tmp)
      self.df = self.df.append(tmp)
      del tmp
#       print('df============ ', self.df)
      return

# if __name__ == '__main__':
def go(cik_list):
   cc = ['company_name','cik','position','cusip','value','percent','tot_value','tot_entries']
   df = pandas.DataFrame([['', '',[],[],[],[],0,0]], columns=cc)
   cnt = 0
   for c in cik_list:
      ticker = cik(c, df)
      if not ticker.validate():
         continue
      ticker.find_first_txt_link()
      ticker.split_txt_file()
      tot_val = ticker.write_primary_doc_csv()
      ticker.write_info_table_csv()
      df = ticker.df.copy()
      cnt += 1
      print(cnt, c)
   df = df[df['company_name'] != '']
#       df = df.append(tmp)
   print(df)
   return df


c = pandas.read_csv("/Users/Dess/Workspace/stratifi-api/data/all_cik_13filers.csv");
c.columns=["a","b"]
l=c.b.tolist()
