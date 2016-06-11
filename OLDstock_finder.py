"""
Module that finds stocks and/or funds. Does bayesian spelling checks, 4-level search.  
"""

import re, collections
import fuzzywuzzy
from ast import literal_eval

def GiveWords(text):
   # we only keep actual words, without fancy characters except amp, whitespace and dash
   return re.findall('[a-z& \-]+', text.lower()) 

def DoTraining(features):
   """
   retrieve weights of each stock
   """
   model = collections.defaultdict(lambda: 1)
   for f in features:
       model[f] += 1
   return model

DICT_STOCKS = DoTraining(GiveWords(open('/Users/Dess/Workspace/stratifi-api/data/new_norm_stocks.txt','r').read()))
alphabet = 'abcdefghijklmnopqrstuvwxyz'

def edits1(word):
   """
   Compute all possible variations with an edit distance of 1
   """ 
   splits     = [(word[:i], word[i:]) for i in range(1,len(word))]
   deletes    = [a + b[1:] for a, b in splits if b]
   transposes = [a + b[1] + b[0] + b[2:] for a, b in splits if len(b)>1]
   replaces   = [a + c + b[1:] for a, b in splits for c in alphabet if b]
   inserts    = [a + c + b     for a, b in splits for c in [alphabet,' ']]
   return set(deletes + transposes + replaces + inserts)

def known_edits2(word):
   """ 
   Same as edits1 but for an edit distance of 2
   """
   return set(e2 for e1 in edits1(word) for e2 in edits1(e1) if e2 in DICT_STOCKS)

def known(words): return set(w for w in words if w in DICT_STOCKS)

def normalize(word):
   """
   Retrieves the most (bayesian) likely variation of an input word 
   """
   word = filter(word)
   candidates = known([word]) or known(edits1(word)) or known_edits2(word) or [word]
   return max(candidates, key=DICT_STOCKS.get)

def filter(stock):
   """
   Filter stocks to remove non-informative bits and split multi-word inputs
   """
   import re
   stock = stock.lower().replace('&amp;','&')
   # we remove non-informative words
   filters = [r'\binc.?\b',r'\bltd.?\b',r'\bhldg.?\b',r'\bholdings?.?\b',r'\bin.?\b',r'\bcompany\b',r'\benterprises?\b',r'\bag.?\b',
              r'\bn ?v.?\b',r'\bsa.?\b',r'\bfunds?\b',r'\bthe\b',r',',r'\bincorporated\b',r'\b-?adr\b',r'\bs ?a\b',r'\bgr\b',
              r'\bag.?\b',r'\bco.?\b',r'\bcorp.?\b',r'\bcorporation.?\b',r'\binternational\b',r'\bfinancials?\b',r'\bllc.?\b',
              r'\bplc.?\b',r'\bgmbh.?\b',r'\.',r'\bii\b',r'\bgroup\b',r'\blt\b',r'\bads\b',r'\bcom\b',
              r'\btr\b',r'\bintl\b',r'\bdel\b',r'\bnew\b'] 
   for aregex in filters:
      stock = re.sub(aregex,'',stock)
   return stock.strip()

def handle_compounds(in_stock):
   """
   Find matching stocks from list of normalized compound stocks 
   """
   in_stock = normalize(filter(in_stock)).split()
   score = 0
   list_found_comp_stocks = []
   for acomp_stock in compound_stocks:
      for subname in in_stock:
         if subname in acomp_stock and len(subname)>1:
#             pdb.set_trace()
            score += 1
      if float(score)/len(in_stock) >= .51:
         list_found_comp_stocks.append(acomp_stock)
      score = 0
   return list_found_comp_stocks
      
#def grep_data(in_stock):
def CreateNormalizedStockList(df):
   """
   Create a list of normalized stocks for Stock Finder Training
   """
   from ast import literal_eval
   #df['filtered_positions'] = df['position'].apply(lambda x: [filter(i) for i in x])
   poz = df.filtered_positions.tolist()
   stocks = []
#    stocks = set( [j[i] for j in poz for i in range(0,len(j))] )
   for p in poz:
      l = literal_eval(p)
      for i in range( 0, len(l) ):
         stocks.append(l[i])
   import codecs, csv
   csvfile = codecs.open('/Users/Dess/Workspace/stratifi-api/data/new_norm_stocks.txt', 'w', encoding='utf-8')
   thewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
   for elem in stocks:
      thewriter.writerow( [elem] )
   csvfile.close()
   return df
   
def ReadNormalizedStockList():
   # read single-word and compound-word stocks from file data/norm_stocks.txt
   normalized_stocks = []
   compound_stocks = []
   import codecs, csv
   f = codecs.open('/Users/Dess/Workspace/stratifi-api/data/new_norm_stocks.txt', 'r', encoding='utf-8')
   reader = csv.reader(f)
   for row in reader:
      normalized_stocks.append( row[0] )
      if len(row[0].split()) > 1:
         compound_stocks.append( row[0] )
   f.close()
   return normalized_stocks, compound_stocks

normalized_stocks, compound_stocks = ReadNormalizedStockList()

def GetFundsFromCusip(in_cusip, df):
   matched_funds = df[df['cusip'].apply( lambda x: any(in_cusip == s for s in x) )]['company_name'].tolist()
   if not matched_funds:
      return 'NONE FOUND', 'TRY AGAIN!'
   the_stock = GetNameFromCusip(matched_funds[0], in_cusip, df)
   out_dict = {}
   for m in matched_funds:
      out_dict[m] = GetRelativeValueFromCusip(m, in_cusip, df)
   return the_stock,out_dict
   
def GetFunds(in_stock, df):
   ##########
   # level 1: exact match (the input stock is somehow normalized already)
   ##########
   the_stock = in_stock.lower()
   if the_stock in normalized_stocks:
      matched_funds = df[df['filtered_positions'].apply( lambda x: any(the_stock in s for s in x) )]['company_name'].tolist()
      outdict = {}
      for m in matched_funds:
         outdict[m] = GetRelativeValue(m, the_stock, df)
      return [the_stock],outdict
   ##########
   # level 2: exact match after filter
   ##########
   the_stock = normalize( filter(in_stock) )
   if the_stock in normalized_stocks:
      matched_funds = df[df['filtered_positions'].apply( lambda x: any(the_stock in s for s in x) )]['company_name'].tolist()
      outdict = {}
      for m in matched_funds:
         outdict[m] = GetRelativeValue(m, the_stock, df)
      return [the_stock],outdict
   ###
   ##########
   # level 3: partial match after filter with compound stocks
   ##########
   candidates = handle_compounds(in_stock)
   if candidates != []:
      matched_funds = []
      for c in candidates:
         matched_funds += df[df['filtered_positions'].apply( lambda x: any(c in s for s in x) )]['company_name'].tolist()
      outdict = {}
      for m in matched_funds:
         outdict[m] = GetRelativeValueFromListOfStocks(m, candidates, df)
      return candidates, outdict
   ########
   # level 4: grep style using filed positions, NOT normalized ones
   ########
   the_stock = in_stock.lower()
   matched_funds = df[df['position'].apply( lambda x: any(in_stock.lower() in s.lower() for s in x) )]['company_name'].tolist()
   outdict = {}
   for m in matched_funds:
      outdict[m] = GetRelativeValueFromRawPosition(m, the_stock, df)
   return [the_stock],outdict
   
def GetMatchingFunds(in_fund,df):
   is2fuzzy = 0
   funds = []
   if len(in_fund.split()) != 2:
      funds = df[df['company_name'].apply( lambda x: (in_fund.lower() in x.lower()) )]['company_name'].tolist()
   if funds == [] and len(in_fund.split())==2:
      in_fund = in_fund.split()
      funds = df[df['company_name'].apply( lambda x: (in_fund[0].lower() in x.lower() and in_fund[1].lower() in x.lower()) )]['company_name'].tolist()
   if funds and len(funds) < 6:
      return is2fuzzy, funds
   from fuzzywuzzy import fuzz
   from fuzzywuzzy import process
   funds = df[df['company_name'].apply( lambda x: ( fuzz.token_sort_ratio(x, in_fund) > 50 and fuzz.token_set_ratio(x, in_fund) > 65 ) )]['company_name'].tolist()
   if not funds or len(funds) >= 10:
      is2fuzzy = 1
      funds = df[df['company_name'].apply( lambda x: ( fuzz.token_sort_ratio(x, in_fund) > 40 and fuzz.token_set_ratio(x, in_fund) > 49 ) )]['company_name'].tolist()
   return is2fuzzy, funds

def GetMatchingFundsFromCIK(in_cik,df):
   fund = df[df['cik'] == int(in_cik)]['company_name'].tolist()[0]
   if not fund:
      return 'NONE FOUND - CHECK CIK'
   return fund

def GetCIKFromMatchingFund(in_fund,df):
   return df[df['company_name'] == in_fund]['cik'].tolist()[0]

def GetCandidateStocks(in_stock, df):
   ##########
   # level 1: exact match (the input stock is somehow normalized already)
   ##########
   the_stock = in_stock.lower()
   if the_stock in normalized_stocks:
      matched_funds = df[df['filtered_positions'].apply( lambda x: any(the_stock == s for s in x) )]['company_name'].tolist()
      cands = []
      for m in matched_funds:
         cands += GetCandPos(m, the_stock, df)
      return cands
   ##########
   # level 2: exact match after filter
   ##########
   the_stock = normalize( filter(in_stock) )
   if the_stock in normalized_stocks:
      matched_funds = df[df['filtered_positions'].apply( lambda x: any(the_stock in s for s in x) )]['company_name'].tolist()
      cands = []
      for m in matched_funds:
         cands += GetCandPos(m, the_stock, df)
      return cands
   ###
   ##########
   # level 3: partial match after filter with compound stocks
   ##########
   candidates = handle_compounds(in_stock)
   if candidates != []:
      cands = []
      matched_funds = []
      for c in candidates:
         matched_funds += df[df['filtered_positions'].apply( lambda x: any(c in s for s in x) )]['company_name'].tolist()
      for m in matched_funds:
         cands += GetCandPosFromListOfStocks(m, candidates, df)
      return cands
   ########
   # level 4: grep style using filed positions, NOT normalized ones
   ########
   the_stock = in_stock.lower()
   matched_funds = df[df['position'].apply( lambda x: any(in_stock.lower() in s.lower() for s in x) )]['company_name'].tolist()
   cands = []
   for m in matched_funds:
      cands += GetCandPosFromRawPos(m, the_stock, df)
   return cands

def GetTotalPos(afund, df):
   # check if that equals df.totval
   return float( df[df.company_name == afund].tot_value.tolist()[0] ) + 1e-9
#    return sum([float(x) for x in df[df.company_name == afund].value.tolist()[0]])

def GetRelativeValue(afund, astock, df):
   """
   Get relative value for a given stock within a given fund.
   @return percentage
   """
   subdf = df.loc[df['company_name']==afund]
   vals = subdf.value.tolist()[0]
   pos = subdf.filtered_positions.tolist()[0]
   bool_list = [astock in i for i in pos]
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   val = sum( [float(vals[i]) for i in index_stock] )
   return round( 100*float(val) / GetTotalPos(afund, df), 2)

def GetRelativeValueFromCusip(afund, acusip, df):
   """
   Get relative value for a given stock within a given fund.
   @return percentage
   """
   subdf = df.loc[df['company_name']==afund]
   vals = subdf.value.tolist()[0]
   cusips = subdf.cusip.tolist()[0]
   bool_list = [acusip in i for i in cusips]
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   val = sum( [float(vals[i]) for i in index_stock] )
   return round( 100*float(val) / GetTotalPos(afund, df), 2)

def GetRelativeValueFromRawPosition(afund, astock, df):
   subdf = df.loc[df['company_name']==afund]
   vals = subdf.value.tolist()[0]
   pos = subdf.position.tolist()[0]
   bool_list = [astock in i for i in pos]
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   val = sum( [float(vals[i]) for i in index_stock] )
   return round( 100*float(val) / GetTotalPos(afund, df), 2)

def GetRelativeValueFromListOfStocks(afund, stocks, df):
   subdf = df.loc[df['company_name']==afund]
   vals = subdf.value.tolist()[0]
   pos = subdf.filtered_positions.tolist()[0]
   bool_list = []
   for p in pos:
      for c in stocks:
         if c in p:
            bool_list.append(True)
            break
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   val = sum( [float(vals[i]) for i in index_stock] )
   return round( 100*float(val) / GetTotalPos(afund, df), 2)

def GetCandPos(afund, astock, df):
   """
   """
   subdf = df.loc[df['company_name']==afund]
   pos = subdf.filtered_positions.tolist()[0]
   truepos = subdf.position.tolist()[0]
   cusip = subdf.cusip.tolist()[0]
   bool_list = [astock in i for i in pos]
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   cands = [truepos[i] for i in index_stock]
   cusip_cands = [cusip[i] for i in index_stock]
   return [(i,j) for i,j in zip(cands, cusip_cands)]

def GetCandPosFromRawPos(afund, astock, df):
   subdf = df.loc[df['company_name']==afund]
   pos = subdf.position.tolist()[0]
   cusip = subdf.cusip.tolist()[0]
   bool_list = [astock in i for i in pos]
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   cands = [pos[i] for i in index_stock]
   cusip_cands = [cusip[i] for i in index_stock]
   return [(i,j) for i,j in zip(cands, cusip_cands)]

def GetCandPosFromListOfStocks(afund, stocks, df):
   subdf = df.loc[df['company_name']==afund]
   pos = subdf.filtered_positions.tolist()[0]
   truepos = subdf.position.tolist()[0]
   cusip = subdf.cusip.tolist()[0]
   bool_list = []
   for p in pos:
      for c in stocks:
         if c in p:
            bool_list.append(True)
            break
   index_stock = [i for i, elem in enumerate(bool_list, 0) if elem]
#    pdb.set_trace()
   cands = [truepos[i] for i in index_stock]
   cusip_cands = [cusip[i] for i in index_stock]
   return [(i,j) for i,j in zip(cands, cusip_cands)]

def GetNameFromCusip(afund, acusip, df):
   subdf = df.loc[df['company_name']==afund]
   pos = subdf.position.tolist()[0]
   cusip = subdf.cusip.tolist()[0]
   for ix,i in enumerate(cusip):
      if acusip == i:
         index_name = ix
         break
   return pos[index_name]

def unique_by_key(elements, key=None):
    if key is None:
        # no key: the whole element must be unique
        key = lambda e: e
    return {key(el): el for el in elements}.values()

