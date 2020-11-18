
import argparse





# ABOUT THIS ETL SCRIPT:
#
# In a real production environment, it should NEVER be necessary to create such a pessimistic ETL script (such as this one); instead, it should be possible 
# to modify the source file export delimiter (e.g. use '\t\t\t' instead of '\t') which then would allow us to process/parse the output more efficiently using the standard Python 'csv' module, 
# which, subsequently, would also give us the ability to parallelize the processing more efficiently (i.e. removing the need to store and parse bytes in advance 
# in order to do lookaheads to try and verify if a \t or \n or \r was unintentional and needs to be escaped, or if it was indeed an intentional separator). Actually, parallelizing such a
# pessimistic parser (as this one is) essentially nullifies all potential gains of parallelization in the first place; the entire file has to be parsed in order to figure 
# out where the file parsing should start and stop, accounting for any "unintentional" \t or \r or \n, meaning every node/process has to read/store/parse the entire file contents.
#
# However, sometimes modifying an export format is not possible (legacy systems, etc), which then necessitates such a pessimistic parser, which brings us here. The below code does NOT currently
# account for all parsing anomalies, but it does demonstrate the logical approach needed to eventually accomplish such a pessimistic parser, given more time to find and handle all the edge cases. It should
# be noted, however, that in some parser anomaly cases, it will be IMPOSSIBLE for a computer to "assume" the correct "intention" of the data, which then means some bad data will inevitably get
# written to the database, which, I would say, is not acceptable for a real production environment ETL script, making any attempt at such a pessimistic parser (like this) ultimately pointless, unless
# and until all edge case anomolies are eventually handled in the parser logic. On a related note, this is why the csv module does not try to parse poorly formatted data such as this 
# (i.e. suppressing invalid chars and/or assuming the stop/starts of columns based on lookahead or other context clues), and instead simply errors out; erroring out is actually the desired behavior 
# to avoid writing bad data to the database, and is the exact opposite of what this ETL script aims to do, which means this script will continue to produce bad output unless and until all edge 
# case anomalies in the file export format are handled (assuming that is even possible, which it might not even be possible, which again points us to modifying the output format as the better solution).
#
# Furthermore, instead of printing out rows (to pipe into, for example, a seperate Redshift COPY script), I would prefer to build the database writing logic
# into this script (around the same area where the full row is printed currently). This would allow for a more "delta" focused approach, where only new/updated rows are written to the database,
# instead of the current implied approach where (I assume) the table is truncated, and then a COPY is performed which INSERTS all rows (truncated from multiple processes or a single process). Typically,
# I will only design ETLs with a "Redshift TRUNCATE & COPY/INSERT INTO" approach if the "delta" approach is not possible (which, I have found, is very rare, as the "delta" approach is usually possible.)








arg_parser = argparse.ArgumentParser(description='Parses data/data.tsv (using optional --position and --length parameters) and prints a cleaned TSV.')

arg_parser.add_argument("--position", type=int, help="byte position (estimated) to start reading (starts at first byte on the line)")
arg_parser.add_argument("--length", type=int, help="number of bytes (estimated) to read (line will be included only if length goes past last byte on the line)")
arg_parser.add_argument("--verbose", action="store_true", help="show debug output in addition to records")


args = arg_parser.parse_args()







byte_array = [] # optimize script for error tolerant position-and-length parsing, not general position-and-length parsing (hard to parse on the fly, so build valid lines instead, using lookahead)
with open("./data/data.tsv", "rb") as file:

    while (byte := file.read(2)): # file is UTF-16-LE, so read 2 bytes at a time, decode

        byte_array.append(byte.decode('utf-16-le'))







# READ START
read_start_position = 0

# if POSITION is specified by user: find beginning of line to start reading
if args.position is not None:
    read_start_position = args.position

# READ END
read_end_position = len(byte_array) - 1

# if LENGTH is specified by user: find end of line to end reading
if args.length is not None:
    read_end_position = read_start_position + args.length








new_line_bytes = ['\n', '\r']
tab_byte = '\t'

is_parsing_record_id = True
record_id = ""

is_parsing_record_first_name = False
record_first_name = ""

is_parsing_record_last_name = False
record_last_name = ""

is_parsing_record_account_number = False
record_account_number = ""

is_parsing_record_email = False
record_email = ""

meta_line_starts_on_byte = 0
meta_line_ends_on_byte = 0

is_parsing_file_header = False
for i in range(0, len(byte_array)):


    if args.verbose: print("---------i:", i, '"', byte_array[i], '"')


    if is_parsing_file_header:

        if args.verbose: print("--parsing file header; skipping--")

        if byte_array[i] in new_line_bytes:

            is_parsing_file_header = False
            record_id = "" # reset id from 'id' to '' for next record
            continue

        else:

            continue


    if byte_array[i] == tab_byte or byte_array[i] in new_line_bytes:

        continue # all tabs & newlines are handled in lookaheads; ignore during parsing



    if is_parsing_record_id:

        record_id += byte_array[i]

        # special check to skip header row (record_id will be the text 'id' instead of a number)
        if record_id + byte_array[i + 1] == "id":

            is_parsing_file_header = True
            continue

        # lookahead to see if tab or newline seperator is next
        if byte_array[i + 1] == tab_byte or byte_array[i + 1] in new_line_bytes:

            # if char after seperator is letter, then first_name is likely (i.e. seperator is intentional)
            if byte_array[i + 2].isalpha():

                is_parsing_record_id = False
                is_parsing_record_first_name = True

            # elif char after seperator is number, then we are still parsing id (i.e. seperator is unintentional)
            elif byte_array[i + 2].isdigit():
                
                pass # we should be erroring here so the imput file can be fixed, instead of silently suppressing the parser anomoly (invalid char in record_id column)



    elif is_parsing_record_first_name:

        record_first_name += byte_array[i]

        # lookahead to see if tab or newline seperator is next
        if byte_array[i + 1] == tab_byte or byte_array[i + 1] in new_line_bytes:

            # if char after seperator is capital letter, then last_name is likely (i.e. seperator is intentional)
            if byte_array[i + 2].isalpha() and byte_array[i + 2].isupper():

                #TODO it is possible for a false positive here 
                #   (first name contains weird chars then more capital letters, 
                #   so lookahead again to see if this is the case, then escape that char as well)

                is_parsing_record_first_name = False
                is_parsing_record_last_name = True

            # else, seperator was unintentional (escape it)
            else:

                record_first_name += byte_array[i + 1].encode('unicode_escape').decode('utf-8')



    elif is_parsing_record_last_name:

        record_last_name += byte_array[i]

        # lookahead to see if tab or newline seperator is next
        if byte_array[i + 1] == tab_byte or byte_array[i + 1] in new_line_bytes:

            # if char after seperator is number, then account_number is likely (i.e. seperator is intentional)
            if byte_array[i + 2].isdigit():

                is_parsing_record_last_name = False
                is_parsing_record_account_number = True

            # else, seperator was unintentional (escape it)
            else:

                record_last_name += byte_array[i + 1].encode('unicode_escape').decode('utf-8')


    elif is_parsing_record_account_number:

        record_account_number += byte_array[i]
        
        # lookahead to see if tab or newline seperator is next
        if byte_array[i + 1] == tab_byte or byte_array[i + 1] in new_line_bytes:

            # if char after seperator is letter, then email is likely (i.e. seperator is intentional)
            if byte_array[i + 2].isalpha():

                is_parsing_record_account_number = False
                is_parsing_record_email = True

            # elif char after seperator is number, then could be either email, or a weird char to suppress silently
            #elif byte_array[i + 2].isdigit():
            else:

                # try to make sure email is likely with 70-char (email RFC says 64 chars is max before '@') lookahead for '@' (i.e. seperator was intentional, email happened to start with a digit, which is legal)
                if '@' in [byte_array[k] for k in range(i + 2, i + 70)]:

                    is_parsing_record_account_number = False
                    is_parsing_record_email = True                


    elif is_parsing_record_email:

        record_email += byte_array[i]

        # lookahead to see if tab or newline seperator is next
        if i == len(byte_array) - 1 or byte_array[i + 1] == tab_byte or byte_array[i + 1] in new_line_bytes:

            # if char after seperator is number or end of file reached, then id for following record is likely, or finished parsing (i.e. seperator is intentional)
            if i == len(byte_array) - 1 or byte_array[i + 2].isdigit():

                #TODO it is possible for a false positive here
                # (email contains number after a weird character)
                # so lookahead and check to make sure we are still parsing email

                meta_line_ends_on_byte = i


                if args.verbose: 
                    print("read_start_position:", read_start_position, "read_end_position:", read_end_position)
                    print("meta_line_starts_on_byte:", meta_line_starts_on_byte, "meta_line_ends_on_byte:", meta_line_ends_on_byte)
                    print(read_start_position <= meta_line_starts_on_byte and read_end_position >= meta_line_ends_on_byte)
                    print("record_id:", record_id)
                    print("record_first_name:", record_first_name)
                    print("record_last_name:", record_last_name)
                    print("record_account_number:", record_account_number)
                    print("record_email:", record_email)



                # print the record, but only if desired read range
                if read_start_position <= meta_line_starts_on_byte and read_end_position >= meta_line_ends_on_byte:

                    print("%s\t%s\t%s\t%s\t%s\r" % (record_id, record_first_name, record_last_name, record_account_number, record_email))

                # re-init record vars and parser state
                is_parsing_record_id = True
                record_id = ""

                is_parsing_record_first_name = False
                record_first_name = ""

                is_parsing_record_last_name = False
                record_last_name = ""

                is_parsing_record_account_number = False
                record_account_number = ""

                is_parsing_record_email = False
                record_email = ""       

                meta_line_starts_on_byte = meta_line_ends_on_byte + 1

            # else, seperator was unintentional (escape it)
            else:

                record_email += byte_array[i + 1].encode('unicode_escape').decode('utf-8')


