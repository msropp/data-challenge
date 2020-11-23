import psycopg2

"""
Hardcoded list of data types in data.tsv
"""
def get_data_types():
            # id       first_name last_name  account_number  email
    return ['smallint', 'varchar', 'varchar', 'smallint', 'varchar']


"""
Grabs all data from tsv file
"""
def get_all_data(filepath):
    with open(filepath, 'r') as datafile:
        return datafile.readlines()


"""
Goes through data to find longest value for each data type

Used for table creation, specifically for varchar types
"""
def find_varchar_lengths(data):
    lengths = [0,0,0,0,0]
    for line in data:
        vals = line.split('\t')
        for i in range(len(vals)):
            val_length = len(vals[i])
            if val_length > lengths[i]:
                lengths[i] = val_length
    return lengths


"""
Builds create table statement to eventually read data into redshift
"""
def create_table(headers, types, lengths):
    statement = "create table user_data ("
    for i in range(len(types)):
        if types[i] == 'varchar':
            statement = (statement + '\n{} varchar({})').format(headers[i].lower(), lengths[i])
        else:
            statement = (statement + '\n{} {},'.format(headers[i].lower(), types[i]))
    return statement[:-1] + ');'


"""
Put data on AWS/S3 to push to redshift
"""
def set_up_data_on_s3(statement, host, user, port, password):
    # set up connection

    connection = psycopg2.connect(
        host=host,
        user=user,
        port=port,
        password=password,
        dbname='tsv_data_db')

    cursor = connection.cursor()

    cursor.execute(statement)
    connection.commit()

    return connection, cursor


"""
Push data from AWS/S3 to redshift
"""
def push_to_redshift(s3_connection, s3_cursor, filepath, access_key_id, secret_access_key):
    sql = """copy tsv_data from '""" + filepath + """'
        access_key_id '""" + access_key_id + """'
        secret_access_key '""" + secret_access_key + """'
        region 'us-west-1'
        ignoreheader 1
        null as 'NA'
        removequotes
        delimiter '\t';"""

    s3_cursor.execute(sql)
    s3_connection.commit()


def main():
    filepath = "./data/data_parsable.tsv"
    data = get_all_data(filepath)
    headers = data[0].replace('\n', '').split('\t')
    types = get_data_types()
    lengths = find_varchar_lengths(data)

    statement = create_table(headers, types, lengths)

    # note: update these values with relevant credentials
    host = 'mydb.mydatabase.us-west-2.redshift.amazonaws.com'
    user = 'user'
    port = 1234
    password = 'password'
    access_key_id = '<access_key_id>'
    secret_access_key = '<secret_access_key>'

    s3_connection, s3_cursor = set_up_data_on_s3(statement, host, user, port, password)
    push_to_redshift(s3_connection, s3_cursor, filepath, access_key_id, secret_access_key)

if __name__ == '__main__':
    main()