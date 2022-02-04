import boto3
import tempfile
import configparser
import numpy as np
import pandas as pd
import json
import requests
from urllib.request import urlopen
from pandas import json_normalize 
from datetime import datetime

class SqsQueue():
    def __init__(self, sqs, queue_name):
        '''Set attributes for an SQS queue object'''
        self.sqs = sqs
        self.name = queue_name
        self.url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        self.size = int(sqs.get_queue_attributes(QueueUrl=self.url, AttributeNames=['ApproximateNumberOfMessages'])['Attributes']['ApproximateNumberOfMessages'])

class ConfigFromS3(object):
    def __init__(self, bucket_name, key, region_name):
        '''Read and parse configuration file from S3'''
        defaults = {'aws_region': region_name}
        session = boto3.session.Session(region_name=region_name)
        self.bucket = session.resource('s3').Bucket(bucket_name)
        temporary_file = tempfile.NamedTemporaryFile()
        self.bucket.download_file(key, temporary_file.name)
        self.config = configparser.RawConfigParser(defaults=defaults)
        with open(temporary_file.name, 'r') as f:
            self.config.read_file(f)
        temporary_file.close()

def remove_regex(df, col_name, rm_words, rm_chars, code_space=True, add_paren=True):
    '''Remove unwanted words and characters from a dataframe column and form query strings for Twitter'''


    # rm_words > rm_chars > rm_words
    # so that 'A A Milner' and 'A. A. Milner' become same person
    # cannot simply be rm_chars > rm_words
    # because 'ph.d.' 
    rm_words = [r.lower() for r in rm_words]                                    # remove regex strings in rm_words
    if len(rm_words)>0:                                                         # iff separated by spaces
        for r in rm_words:                                                      # or located at beginning or end of string
            df[col_name] = df[col_name].replace(' '+ r +' ',' ',regex=True)\
                            .replace(' '+ r +'$','',regex=True)\
                            .replace('^'+ r +' ','',regex=True)\
                            .replace('  ',' ',regex=True)
                            
    if len(rm_chars)>0:                                                         # remove regex strings in rm_chars
        for r in rm_chars:                                                      # words are removed again so that
            df[col_name] = df[col_name].replace(r,' ',regex=True)               # 'A A Milner' and 'A. A. Milner' become same person
        
    if len(rm_words)>0:                                                         # words are removed again so that
        for r in rm_words:                                                      # 'A A Milner' and 'A. A. Milner' become same person
            df[col_name] = df[col_name].replace(' '+ r +' ',' ',regex=True)\
                            .replace(' '+ r +'$','',regex=True)\
                            .replace('^'+ r +' ','',regex=True)\
                            .replace('  ',' ',regex=True)
                            
    df[col_name] = df[col_name].str.replace('\W',' ', regex=True)               # keep alphanumeric characters only
    for i in range(10):
        df[col_name] = df[col_name].str.replace('  ',' ', regex=True)           # make sure there aren't 2+ spaces
    df[col_name] = df[col_name].str.strip()
    if code_space:
        df[col_name] = df[col_name].replace(' ','%20',regex=True)               # replace space with '%20'
    if add_paren:
        df[col_name] = ['(' + q + ')' for q in df[col_name]]                    # encase string in parentheses
    return df       
    
def wait_query_success(athena, response):
    '''Check athena query status until it returns "SUCCEEDED"'''
    status=''
    while status != 'SUCCEEDED':
        query_execution = athena.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        status = query_execution['QueryExecution']['Status']['State']
        if status == 'QUEUED' or status == 'RUNNING':
            print(status)
        if status == 'FAILED' or status == 'CANCELLED':
            print(status + '\n')
            print(query_execution)
            raise Exception(query_execution)
    return None

def athena_start_query_execution(athena, query, path):
    '''Execute Athena query with given query and output path and wait for success'''
    response = athena.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': f'{path}'},
        WorkGroup='primary',
        QueryExecutionContext={'Catalog': 'AwsDataCatalog','Database': 'ccindex'}
    )
    wait_query_success(athena,response)
    return None

def query_builder(query_type, db, table, catalog='', columns=[], bucket='', key='', formatting='', select='', where=''):
    '''Custom query builder for Athena. Currently supports DROP, CREATE, and UNLOAD'''
    if query_type.lower() == 'drop':
        query = f'''DROP TABLE IF EXISTS {db}.{table}'''
    elif query_type.lower() == 'create':
        query = f'''
            CREATE TABLE {db}.{table}
            WITH ( format='{formatting}', external_location='s3://{bucket}/{key}') AS
            SELECT {columns}
            FROM {catalog}.{db}
        '''
        if where != '': query += f' WHERE {where}'
    elif query_type.lower() == 'unload':
        query = f'''
            UNLOAD (SELECT {select} FROM {table}) 
            TO 's3://{bucket}/{key}' WITH (format='{formatting}')
        '''
    else:
        raise('Query type not supported.')
    return query
    
def get_latest_common_crawl(collinfo):
    '''Get latest crawl name from Common Crawl'''
    response = urlopen(collinfo)
    response_json = json.loads(response.read())
    return response_json[0]['id']
    
def request_ISBNDB(df, request_url, isbn_token, chunk_length):
    '''Chunk single-column dataframe according to chunk_length and request ISBNDB for book data'''
    factor = df.shape[0]/(chunk_length-1)                                           # chunk
    isbn_chunks = np.array_split(df,factor)                                         
    isbn_chunks_joined = [",".join(chunk.isbn.tolist()) for chunk in isbn_chunks]
    booksdf = pd.DataFrame()                                                        # dataframe to be returned                                                       
    headers = {'Authorization': isbn_token, 'accept': 'application/json', 'Content-Type': 'application/json'}
    for chunk in isbn_chunks_joined:
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ': currently in isbn loop')
        data = f'isbns={chunk}'                                                     # request each chunk
        response = requests.post(request_url, headers=headers, data=data)
        booksdf = booksdf.append(json_normalize(response.json(),'data'))            # append response to dataframe
    return booksdf

def sns_publish(sns, sns_batches, topic_name_with_extension):
    '''
    Batch-publish twitter api queries to an SNS topic
    Input: list of sns message dictionaries in sns publish_batch format
    Output: number of failed messages
    '''
    topics = sns.list_topics()
    fail_count = 0
    for topic in topics['Topics']:
        if topic['TopicArn'].split(':')[-1]==topic_name_with_extension:
            topic_arn = topic['TopicArn']
            print(topic_arn)
    if topic_arn is None:
        raise Exception('Please create topic "prepbatch.fifo" through the AWS SNS Console.')
    else:
        for batch in sns_batches:
            response = sns.publish_batch(TopicArn=topic_arn, PublishBatchRequestEntries=batch)
            if len(response['Failed'])>0:
                print(response['Failed'])
                fail_count += 1
    return fail_count

def get_sns_batches(queries):
    '''Transform dataframe into a list of dictionaries formatted for SNS batch-publishing.'''
    qlists = [queries[i:i+10] for i in range(0, len(queries),10)]
    sns_batches =[]
    fail_count = 0
    for i, qlist in enumerate(qlists):
        sns_batch = []
        for j, q in enumerate(qlist):
            sns_batch.append({'Id': str(j), 'Message': q, 'MessageGroupId': str(i)})
        sns_batches.append(sns_batch)
    return sns_batches

def build_tweet_counts_query(q_list):
    '''
    Take a list of queries and combine them with "or" logic, twitter-style
    Input: q_list: list or pandas series of strings
    Output: df: pandas dataframe with a column of queries
    '''
    max_q = 512  # Max query length according to Twitter
    q_list = [q for q in q_list if len(q)<max_q]  # Remove if single query string > max query length
    query_list = [] 

    # Build queries
    iter_q = iter(q_list)
    for q in iter_q:
        query = ''
        while (len(query + "%20OR%20" + q) < max_q):  # Keep query under max length
            if query == '': query += q
            else: query = query + '%20OR%20' + q  # Build query with "OR" logic
            try: q = next(iter_q)
            except: break  # Generator exhausted  
        query = "https://api.twitter.com/2/tweets/counts/recent?query=" + query
        query_list.append(query)

    return query_list 

def explode_query(query_list):
    '''Get individual book queries from bookset queries'''
    exploded_list=[]
    for query in query_list:
        base_url = query.split('=')[0]
        for q in query.split('=')[1].split('%20OR%20'):
            exploded_list.append(base_url + '=' + q)
    return exploded_list

def delete_msg_from_queue(sqs, queue, receipt_handle):
    '''Delete individual messages from an sqs queue and check for success'''
    response = sqs.delete_message(
        QueueUrl=queue.url,
        ReceiptHandle=receipt_handle
    )
    if int(response['ResponseMetadata']['HTTPStatusCode']) != 200:
        raise Exception(f'Message receipt handle {receipt_handle} could not be deleted')