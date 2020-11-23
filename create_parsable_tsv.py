#!/usr/bin/env python


"""
usage: python3 create_parsable_tsv.py path/to/file
"""

import sys
import re


"""
Takes filename and returns entire contents

Note: filename must end with '.tsv' and first line must match (after cleaning)
        id\tfirst_name\tlast_name\taccount_number\temail
"""
def get_file_contents(filename):
    with open(filename, 'r', encoding='utf-16le') as tsv_file:
        return tsv_file.readlines()


"""
Takes original data from file and turns it into a string that looks like:
        1\tFistName\tLastName\t123456\temail@abc.com
"""
def clean_line(line_of_data):
    # line should always look like: 1\tFistName\tLastName\t123456\temail@abc.com
    fixed_data = line_of_data.replace('\x00','')
    while '\t\t' in fixed_data:
        fixed_data = fixed_data.replace("\t\t", "\t\'\\t\'\t")

    return '\t'.join(fixed_data.split())


"""
Regex list works for this file only - would change for other files

Specified for: id first_name last_name account_number email
"""
def get_hardcoded_table_data_regex():
    return ['^[0-9]+', '[a-zA-Z]+', '[a-zA-Z]+', '^[0-9]', '\S+@\S+']


"""
Checks line for too many/not enough values and deletes/adds fields
"""
def fix_line_item_issues(item):
    regexes = get_hardcoded_table_data_regex()

    values = item.split()
    for i in range(len(regexes)):
        regex = regexes[i]
        val = values[i]
        if not re.search(regex, val):
            if len(regexes) > len(values):
                values.insert(i, '\t')
            else:
                values.remove(val)
        else:
            if i == 1 or i == 2:
                if '\\t' not in val:
                    values[i] = val.title()
            elif i == 4:
                values[i] = val.lower()

    return '\t'.join(values)


"""
Loops through entire file contents and creates new, cleaned contents
"""
def create_parsable_file_contents(file_contents):
    # special case for file header
    header = clean_line(file_contents[0])
    new_contents = [header +'\n']

    last_str = ""
    last_num = 1

    for i in range(1, len(file_contents)):
        # remove null characters in line
        cleaned_line = clean_line(file_contents[i])

        # start of new data entry found
        if cleaned_line.startswith(str(last_num+1)):
            new_contents.append(fix_line_item_issues(last_str) + '\n')

            # special case - last line in file
            if i == len(file_contents)-1:
                new_contents.append(fix_line_item_issues(cleaned_line))
                return new_contents

            last_num += 1
            last_str = cleaned_line
        else:
            # special case - don't include \t for header (line 1)
            last_str += '\t' + cleaned_line if i != 1 else cleaned_line

    return new_contents


"""
Writes new, cleaned contents to file with name "path/to/file_parsable.tsv"
"""
def create_parsable_file(file_contents, filename):
    parsable_filename = filename.replace('.tsv', '_parsable.tsv')
    with open(parsable_filename, 'w', encoding='utf-8') as new_tsv_file:
        new_tsv_file.writelines(file_contents)


def main():
    filename = sys.argv[1]  # 1st argument should be path/to/filename.tsv
    file_contents = get_file_contents(filename)
    parsable_file_contents = create_parsable_file_contents(file_contents)
    create_parsable_file(parsable_file_contents, filename)


if __name__ == '__main__':
    main()