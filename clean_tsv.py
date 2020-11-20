def col_len(tlist):
    length = 0
    for i in tlist:
        length = length + len(i)
    return length

#change below open path to input file location
with open("C:/Users/vwing/Desktop/data/data.tsv", "r", encoding='UTF-16LE') as in_file:
    # get header and its length
    data = in_file.readlines()
    i = 0
    while i < len(data):
        # iterate through each row of data
        data_list = data[i].strip().split('\t')
        new_list = []
        new_list.append(data_list)
        length = col_len(new_list)
        # correct each row of data to re-wrtie up to 5 columns
        if length < 5:
            while length < 5:
                i = i + 1
                new_list.append(data[i].strip().split('\t'))
                length = col_len(new_list)
                if length == 5:
                    if len(data[i+1].strip().split('\t')) == 1:
                        i = i + 1
                        new_list.append(data[i].strip().split('\t'))
                        length = col_len(new_list)
            data_list = []
            if col_len(new_list) == 5:
                for j in new_list:
                    data_list = data_list + j
            # if column count is over 5 it's because of an error in name columns
            elif col_len(new_list) == 6:
                # if multiple last names and not equal then here we will concatenate them, else assumed duplicate
                if len(new_list[0]) == 3:
                    new_list[0][2] = new_list[0][2]+' '+new_list[1][0]
                    new_list[1].pop(0)
                    for j in new_list:
                        data_list = data_list + j
                elif len(new_list[0]) == 2 and len(new_list[1]) == 4:
                    # if two equal first names
                    if new_list[0][-1].strip() == new_list[1][0].strip():
                        new_list[1].pop(0)
                    # if two equal last names
                    elif new_list[1][0].strip() == new_list[1][1].strip():
                        new_list[1].pop(0)
                        for j in new_list:
                            data_list = data_list + j

                    else:
                        print('*'*80)
                        print('exception: {0}'.format(new_list))
                else:
                    print('*'*80)
                    print('exception: {0}'.format(new_list))
            else:
                print('*'*80)
                print('exception: {0}'.format(new_list))

        with open('data_utf8.tsv', 'a', encoding='UTF-8') as out_file:
            line = '\t'.join(data_list)
            line = line+'\n'
            out_file.write(line.replace("-", '').replace('/',''))
            i = i + 1
out_file.close()
in_file.close()