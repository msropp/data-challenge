import csv
from  typing  import Generator, List, Union, Set

def remove_duplicate_keep_order(seq:List[str]) -> List[str]:
    seen:Set[str] = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def datagenerator(tsv_file_location:str) -> Generator[List[str], None, None]:
    openedfile = open(tsv_file_location,'r', encoding='utf-16-le')
    data = csv.reader(openedfile, delimiter="\t", quotechar='"')
    
    badrows:List[List[str]] = []
    
    for row in data:
        if len(row) == 5:
            badrows = []
            yield row
            
        else:
            
            row = [w.replace(" ","").replace('\n','') for w in row]
            row = remove_duplicate_keep_order(row)            
            if "" in row:
                row.remove("")
                
            badrows.append(row)
            
            if sum([len(row) for row in  badrows ] ) == 5:
                newrow = []
                for row in badrows:
                    newrow.extend(row)                    
                    badrows = []
                yield newrow
            
if __name__ == "__main__":
    input_file_location_relative = "data\data.tsv"
    output_file_location_relative = "data\output.csv"

    with open(output_file_location_relative, 'wt', newline='') as out_file:
        csv_writer = csv.writer(out_file, delimiter=',', quotechar='"')

        for row in datagenerator(input_file_location_relative):
            csv_writer.writerow(row)
