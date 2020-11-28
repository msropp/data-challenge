# =============================================
# Filename:		convert-data.py
# Author: 		Peter Kim
# Date:			Nov 28, 2020
# Description:	Convert data.tsv into standard CSV file.
# =============================================
'''
See also:
* peterkim-solution.md - plaintext explanation
* notebook/explore-data-challenge.ipynb - exploratory notebook

Dependencies:
* standard Python/Anaconda distribution
* pip install unidecode
* pip install pandas-redshift
'''

# import libraries
import os
import io
import pandas as pd
import numpy as np
import unidecode
import re

# pandas display settings
pd.set_option("display.max_columns", 999)
pd.set_option("display.max_rows", 999)


def clean_anomalies_str(str_anomalies):
	'''
	Function to clean str with anomalies, i.e. 4 tab-delimiters.
	Use string methods and regular expressions to extract records.
	If the record has more than 2 names, drop the middle name so only 2 names.
	Output list of str items, each with 4 tab-delimiters.

	Dependencies:
		* import re
	Input: 
		* str_anomalies - str, records that should have 4 tab-delimiters with data fields.
	Return:
		* ls_clean_4tabs - list, with str items with 4 tab-delimiters and data fields.
	'''

	# clean string
	# (a) replace newline with tab, (b) remove whitespaces, (c) replace double-tab with single-tab
	str_anom_tab_delim_clean = str_anomalies.replace('\n', '\t').replace(' ', '').replace('\t\t', '\t')

	# use regex groups to identify records
	# https://www.tutorialspoint.com/What-is-the-groups-method-in-regular-expressions-in-Python
	'''
	* id - one or more digits
	* name - may include 0, 1, 2, 3 names
	* account number - one or more digits that may contain '-' or '/'
	* email may contain one or more '@', '.'
	'''
	r_str_match_0names = '(\d+\\t[0-9-/]*\\t[a-zA-Z@.]*)'
	r_str_match_1names = '(\d+\\t[a-zA-Z]+\\t[0-9-/]*\\t[a-zA-Z@.]*)'
	r_str_match_2names = '(\d+\\t[a-zA-Z]+\\t[a-zA-Z]+\\t[0-9-/]*\\t[a-zA-Z@.]*)'
	r_str_match_3names = '(\d+\\t[a-zA-Z]+\\t[a-zA-Z]+\\t[a-zA-Z]+\\t[0-9-/]*\\t[a-zA-Z@.]*)'

	# create list of records
	ls_re_find_0names = re.findall(r_str_match_0names, str_anom_tab_delim_clean)
	ls_re_find_1names = re.findall(r_str_match_1names, str_anom_tab_delim_clean)
	ls_re_find_2names = re.findall(r_str_match_2names, str_anom_tab_delim_clean)
	ls_re_find_3names = re.findall(r_str_match_3names, str_anom_tab_delim_clean)

	# clean 3 names by removing middle name
	ls_convert_3to2_names = []
	for each_3name in ls_re_find_3names:
		
		# convert to list, split on tab delimiter
		temp_list = each_3name.split('\t')
		
		# remove middle name
		del temp_list[2]

		# convert to str, tab-delimited
		str_3to2_names = '\t'.join(temp_list)

		# append to list of 3to2 names
		ls_convert_3to2_names.append(str_3to2_names)

	# combine lists for merged output
	ls_regex_clean_4tabs = ls_re_find_2names + ls_convert_3to2_names

	# add newline character to end of str
	ls_regex_clean_4tabs = [x+'\n' for x in ls_regex_clean_4tabs]

	return ls_regex_clean_4tabs


def importlist_cleanpandas_export_csv(list_of_strings):
	'''
	Import the list of strings (tab-delimited) into pandas.
	Clean the data, e.g. account_numbers.
	Could .fillna() on first_name or last_name if preferable.
	Export as CSV to the "data" folder.
	'''

	# convert list to string, already has newline character, no need to add to join
	str_lines_tsv_4tabs = ''.join(list_of_strings)

	# convert str to io.StringIO object so it can be read as CSV file
	# https://www.kite.com/python/answers/how-to-create-a-pandas-dataframe-from-a-string-in-python
	io_data_tsv = io.StringIO(str_lines_tsv_4tabs)

	# create dataframe, tab-delimited
	df_4_tabs = pd.read_csv(io_data_tsv, sep='\t')

	# clean account_number: leading/trailing whitespace, dash, slash ...
	df_4_tabs['account_number'] = df_4_tabs['account_number'].str.replace(' ', '').str.replace('-', '').str.replace('/', '')

	# sort dataframe by 'id' column
	df_4_tabs = df_4_tabs.sort_values('id')
	
	# export as csv
	path_to_csv_export = os.path.join('data', 'data-solution.csv')
	df_4_tabs.to_csv(path_to_csv_export, index=False)


def main():
	'''
	Most of the code is in the main() function.

	Write a separate function for the regex, so it could be used in parallel algorithm.
	'''

	# ==========================================================
	# Section 1: read text data from file
	# ==========================================================

	# create path to tsv file
	path_to_tsv = os.path.join('data', 'data.tsv')

	# read each line of file into a list, challenge documentation said 'utf-16-le' encoding
	# https://realpython.com/read-write-files-python/
	# https://stackoverflow.com/questions/4190683/python-string-replace-for-utf-16-le-file
	with open(path_to_tsv, 'r', encoding='utf-16-le') as f:
		ls_lines_tsv_utf16le = f.readlines()

	# make sure each item ends with newlines
	ls_lines_tsv_newline_utf16le = [x if x.endswith('\n') else x+'\n' for x in ls_lines_tsv_utf16le]

	# remove accents on characters
	# https://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-normalize-in-a-python-unicode-string
	# https://medium.com/@randombites/how-to-handle-accented-special-strings-175e65d96123
	# https://stackoverflow.com/questions/31207287/converting-utf-16-to-utf-8
	ls_lines_tsv = [unidecode.unidecode(x) for x in ls_lines_tsv_newline_utf16le]

	# create list with only items with 4 tabs
	ls_lines_tsv_4tabs = [x for x in ls_lines_tsv if x.count('\t')==4]

	# create list with anomalies '\t'
	ls_lines_tsv_not4tabs = [x for x in ls_lines_tsv if x.count('\t') != 4]

	# ==========================================================
	# Section 2: clean data without 4 tab-delimiters, merge back into one list
	# Use custom function clean_anomalies_str().
	# ==========================================================

	# convert anomalies list to string
	str_tsv_not4tabs = ''.join(ls_lines_tsv_not4tabs)

	# clean anomalies string
	ls_lines_tsv_clean4tabs = clean_anomalies_str(str_tsv_not4tabs)

	# merge into one list
	ls_lines_tsv_pandas = ls_lines_tsv_4tabs + ls_lines_tsv_clean4tabs

	# ==========================================================
	# Section 3: load list into pandas, clean in pandas, export to CSV
	# Use custom function importlist_cleanpandas_export_csv().
	# ==========================================================

	# import list into pandas, clean in pandas, export to CSV
	importlist_cleanpandas_export_csv(ls_lines_tsv_pandas)
	

if __name__ == '__main__':
	main()