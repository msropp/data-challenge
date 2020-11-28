# Peter Kim Solution

### Approach Overview
Examined the data.tsv file.  Rather than immediately working on the solution, I performed significant exploratory data analysis (EDA) on the data using Python Jupyter Notebook; allocated approximately 4-6 hours to simply understanding the data.  Analyzed 1,008 lines by reading text into a list of strings.  Created a 'notebook' folder which contains the EDA in 'explore-data-challenge.ipynb'.

### Major Insight from EDA
Out of 1,008 lines in the dataset, 995 lines have 4 tab-delimiters.  Filtering only those 995 lines, the data is easily read into a pandas dataframe using io.StringIO(), and specifying pandas 'sep' as tab-delimiter.  Once data fits into a dataframe, the remaining steps to clean-transform-export can be performed in pandas.  For example, the 'account_number' column showed some anomalies that would benefit from cleaning.  Also, some records had null values (e.g. first_name, last_name, or both) which can be filled if preferable.

### Difficult Cleaning
But what about the 13 lines (out of 1,008 lines) that do not have 4 tab-delimiters?  The difficult task is to clean-transform those 13 lines so that they fit into the dataframe too (along with the other 995 lines).  Used str cleaning and regular expressions (regex) match groups to extract the records from the 13 lines in a structured way; specifically there were two major types of problems:

1. Delimiter str issues: 
	* Presence of newline characters, where it should have been tab-delimiters
	* Presence of unnecessary extra whitespaces
	* Presence of double-tab-delimiters, where it should have been single-tab-delimiters
2. Data issues: some records had 3 names (where should be maximum 2 names)

### Minor Issue of Accented Character
In the course of experimenting with regex, discovered presence of accented character (understandable given UTF-16-le encoding).  Added conversion to UTF-8 to the beginning of the data pipeline.  

### Solution Algorithm
1. Read data.tsv into list of strings, clean accented character.
1. Divide the list of strings into 2 lists: (a) records with 4 tab-delimiters, (b) records without 4 tab-delimiters.
1. Extract data from the records without 4 tab-delimiters using string cleaning and regular expressions, so the list output has 4 tab-delimiters.
1. Merge the list output together so all records have 4 tab-delimiters.
1. Read the list into pandas dataframe, perform cleaning in pandas.
1. Export the final solution to data-solution.csv

### Bonus
1. Uploading data to Redshift usually involves using pscopg2 library; provide AWS IAM credentials for S3 access, and AWS Redshift credentials.  A convenient wrapper exists in the pandas-redshift library for automating pandas-to-redshift upload, and redshift-to-pandas download.  Described in 'explore-data-challenge.ipynb'.
1. Parallelizing the algorithm could be solved by extracting the string text using .read() instead of .readlines(), and performing regex match groups on the extracted text.  And the final step read the list from regex .findall() into pandas dataframe for further cleaning and then export to .tsv file.