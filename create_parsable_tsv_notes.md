### Script Usage Notes

Usage: `python3 convert_to_tsv.py path/to/file`

Creates a new file in the same folder as the original data called filename_parsable.tsv
(note: filename must end with ".tsv")

Anomalies found in the original data.tsv:
* Null characters had to be removed
* Unnecessary newline characters within lines of data had to be dealt with
  * In all cases, data was concatenated across lines
* Some names included more or less than first_name/last_name. For example:
  * Adena Hobbs Bosley (line 29) - name became "Adena Hobbs"
  * Boris Harrington Harrington (line 217) - name became "Boris Harrington"
  * Copeland (line 302) - name became "Copeland '\t'"
  
A Redshift data creation script is also included.
The `upload_to_redshoft.py` file first pushes the data to AWS/S3, then copies it to Redshift.
* Note: credentials must first be updated in the file before running.