#!flask/bin/python
"""
Module that launches a server instance of the wsig app (based on Flask). 
It allows queries on the 13F database's latest filings.
"""
from flask import Flask, jsonify, abort, request, url_for
import stock_finder

#instance of flask app that launches the server
app = Flask(__name__)

def GetPositions(afund):
	"""
	Returns all positions of a given fund afund
	
	Keyword arguments:
	@param afund str: name of the fund
	@return out_data list: a python list of positions held by the fund
	"""
	# making sure we work with unicode (ie str in python 3.x)
	afund = str(afund)
	# do a pandas search for the fund (company) name.
	try:
		# case insensitive search
		out_data_poz = df.loc[df['company_name'].str.lower() == afund.lower()]['position'].tolist()[0]
		out_data_percents = df.loc[df['company_name'].str.lower() == afund.lower()]['percent'].tolist()[0]
		out_data = [str([i,j]) for i,j in zip(out_data_poz,out_data_percents)]
	# pandas returns IndexError if none is found
	except IndexError:
		out_data = [' ** No Positions in the 13F db ** Check spelling or edgar data.']
	return out_data

# def GetFunds(astock):
# 	"""
# 	Returns all funds that holds a position in the stock astock
# 	Keyword arguments:
# 	@param astock str: name of the stock @todo use TICKER name instead
# 	@return out_data list: a python list of funds holding this stock astock
# 	"""
# 	try:
# 		# case insensitive search for an exact stock name match
# 		out_data = df[df['position'].apply( lambda x: any(astock.lower() == s.lower() for s in x) )]['company_name'].tolist()
# 	except:
# 		out_data = ' ** No fund holds '+astock+' in the 13F db ** Check spelling or edgar data.'
# 	return out_data

@app.route('/query')
def query():
	"""
	query interface with the end-user.
	can be invoked through curl using e.g. curl -X GET http://127.0.0.1:5000/query?stock='XXXX+INC' where + denotes a space
	calls respective functions to retrieve values
	Keyword arguments:
	@return jsonified list of either stock positions for a given fund or funds for a given stock

	"""
    # here we want to get the value of either fund or stock (e.g. ?fund='some-fund')
	global df
	from operator import itemgetter
	if request.method != 'GET':
		# @todo: force that output to json for uniformity of output
		abort(404)
	in_cusip = str( request.args.get('cusip'))
	in_cand = str( request.args.get('candidates'))
	in_fund = str( request.args.get('fund') )
	in_cik = str( request.args.get('cik') )
	in_stock = str( request.args.get('stock') )
	if in_fund != 'None':
		# get a list of matching funds
		is2fuzzy, matched_funds = stock_finder.GetMatchingFunds(in_fund,df)
		matched_funds.sort()
		# for each of matching fund, get list of positions
		if not is2fuzzy:
			out_dict = {}
			out_dict = {afund:GetPositions(afund) for afund in matched_funds}
			return jsonify( { 'Fund '+in_fund: out_dict } )
		else:
			out_list = [str( (m,stock_finder.GetCIKFromMatchingFund(m,df)) ) for m in matched_funds]
			return jsonify( { 'Fund queried: '+in_fund+' IS TOO FUZZY -- Please be more accurate -- closest candidates so far, with CIK: ': out_list } )
	if in_cik != 'None':
		# get a list of matching funds from CIK
		matched_fund = stock_finder.GetMatchingFundsFromCIK(in_cik,df)
		return jsonify( { 'Fund with CIK= '+in_cik: matched_fund } )
	if in_stock != 'None':
		candidate_stocks, out_data = stock_finder.GetFunds(in_stock, df)
		candidates = ' ; '.join(candidate_stocks)
		return jsonify( { 'QUERIED Stock: '+in_stock+' --- STOCK CANDIDATES: '+candidates: out_data } )
	if in_cusip != 'None':
		the_stock, out_data = stock_finder.GetFundsFromCusip(in_cusip, df)
		return jsonify( { 'QUERIED CUSIP: '+in_cusip+' --- STOCK: '+the_stock: out_data } )
	if in_cand != 'None':
		list_cands = stock_finder.GetCandidateStocks(in_cand, df)
		set_unique_cusip = list( stock_finder.unique_by_key(list_cands, key=itemgetter(1)) )
		set_unique_cusip.sort()
		return jsonify( { 'QUERIED Stock: '+in_cand+' --- STOCK CANDIDATES: ': set_unique_cusip } )

@app.route('/')
def index():
    return "Hello, Stratifi!\n"

@app.route('/update')
def update():
	"""
	"""
	global df
	df = read_data()
	return "data updated...\n"

@app.route('/dumpfunds')
def dump_funds():
	"""
	dump the fund names 
	"""
	return jsonify( {'Funds loaded:':df.company_name.tolist()} )

@app.route('/normalize')
def normalize():
	in_stock = str( request.args.get('stock') )
	return jsonify( { 'Stock: '+in_stock+' == ': stock_finder.normalize(in_stock) } )

def read_data(path_to_data = 'data/data_13F.json'):
	"""
	Read and parse the 13F edgar data. Data were crawled with ParseHub for the last Q15' (from 12/31/15 to 2/18/16). About 4100 filings. 
	Keyword arguments:
	@param path_to_data str: path to the json data file
	@return df pandas.DataFrame: a pandas data frame with the 13F data 
	"""
	import pandas, json
	from stock_finder import filter
	from ast import literal_eval
	# read json data and store it into a DataFrame df
# 	with open(path_to_data) as data_file:
# 		data = json.load(data_file)
# 	df = pandas.DataFrame( data['list_info_table'] ).dropna()
	# de-listify the first column (for easier indexing)
# 	df['company_name'] = df.apply(lambda x: x[0][0], axis=1)
	df = pandas.read_csv('data/new_data_full.csv').dropna()
	# some preprocessing for faster queries
	df['value'] = df['value'].apply(lambda x: literal_eval(x))
	df['position'] = df['position'].apply(lambda x: literal_eval(x))
	df['cusip'] = df['cusip'].apply(lambda x: literal_eval(x))
	df['percent'] = df['percent'].apply(lambda x: literal_eval(x))
	df['filtered_positions'] = df['filtered_positions'].apply(lambda x: literal_eval(x))
# 	df['filtered_positions'] = df['position'].apply(lambda x: [filter(i) for i in x])
	#p = df[index_company:index_company+1]['position']
	return df	

if __name__ == '__main__':
	"""
	Main scope. 
	"""
	# made global so that it can be updated if necessary
	global df
	# get 13F json data into a pandas data frame 
	df = read_data()
	# launch the app 
	app.run(debug=True)
