import boto3
import time
import pandas as pd
import awswrangler as wr
from datetime import datetime
from lib import *

SNS_FAIL_COUNT = -1
BUCKET="warcbooks"
REGION='us-east-1'
CONF = ConfigFromS3(BUCKET, 'script/config/hb.cfg', REGION).config 
TWITTER_BEARER = CONF.get('Twitter','Bearer')
VERSION = 'cur_version'
SQS = boto3.client('sqs')
SNS = boto3.client('sns')
GLUE = boto3.client('glue')

def lambda_handler(event, context):
    '''
    Reads book data from S3 and queries twitter for mentions.
    In the first iteration, each query asks about a set of roughly ten books ("bookset").
    This is done in order to speed up the batching process while dealing Twitter API limits.
    The top 100 booksets, by mention count, are blown up into roughly 1,000 individual book queries.
    Top 100 books, by mention count, are stored in a json file on S3.
    '''
    # In case the counting functions don't run properly
    counts_df_length=-1
    book_counts_df_length=-1
    
    # queue objects
    prepbatch = SqsQueue(sqs=SQS, queue_name='prepbatch.fifo')
    batchbook = SqsQueue(sqs=SQS, queue_name='batchbook.fifo')
    
    # version-controlled paths
    book_counts_all_path = f's3://{BUCKET}/data/extracted/twitter/book_counts/all/{VERSION}'
    book_counts_most_recent_path = f's3://{BUCKET}/data/extracted/twitter/book_counts/most_recent'
    book_counts_csv_path = f's3://{BUCKET}/data/extracted/twitter/book_counts/csv'
    book_skipped_df_path = f's3://{BUCKET}/data/extracted_failed/twitter/book_counts/all/{VERSION}'
    bookset_counts_all_path = f's3://{BUCKET}/data/extracted/twitter/bookset_counts/all/{VERSION}'
    bookset_counts_most_recent_path = f's3://{BUCKET}/data/extracted/twitter/bookset_counts/most_recent'
    bookset_skipped_df_path = f's3://{BUCKET}/data/extracted_failed/twitter/bookset_counts/all/{VERSION}'
    topbooks_all_path = f's3://{BUCKET}/data/extracted/twitter/topbooks/all/{VERSION}'
    topbooks_most_recent_path = f's3://{BUCKET}/data/extracted/twitter/topbooks/most_recent'
    print(prepbatch.size)
    print(batchbook.size)
    if prepbatch.size == 0:
        if batchbook.size == 0:
            recent_file = wr.s3.list_objects(book_counts_most_recent_path)
            if not recent_file:
                print('where')
                # If the prepbatch and batchbook queues both happen to be empty, 
                # and we haven't queried twitter for mentions of individual books 
                # (as shown by the lack of recent book counts file), 
                # then fill the batchbook queue using recent bookset data on S3.
                sns_publish_to_batchbook() # Explode top 100 booksets to individual books, publish to batchbook
            else:
                print('oh')
                top_file = wr.s3.list_objects(topbooks_most_recent_path)
                if not top_file:
                    # Prepbatch and batchbook queues are empty, book counts files have been made,
                    # but topbooks files have not been made yet
                    # Time to select top 100 books.
                    booksdf = wr.s3.read_json(path=book_counts_most_recent_path, dtype=False)
                    booksdf = booksdf.nlargest(300,'total_count')
                    booksdf = booksdf.reset_index().drop(columns="index")
                    wr.s3.to_json(df=booksdf, path=topbooks_all_path, dataset=True)
                    wr.s3.to_json(df=booksdf, path=topbooks_most_recent_path, dataset=True)
                    # Do not delete the most recent book counts file even after processing it
                    # Lambda function 'twitterbooks' will take care of it next time
                    
                    # Invoke lambda function main_batch_topbooks
                    print('here')
                    l = boto3.client('lambda')
                    response = l.invoke(FunctionName='main_batch_topbooks')
        else:
            # Prepbatch queue is empty, but batchbook queue has queries for individual books that have made the first cut. 
            # Time to query twitter and store results.
            book_counts_df, book_skipped_df, _ = query_twitter_total_count(batchbook)
            book_counts_df_paths = [book_counts_all_path, book_counts_most_recent_path]
            write_counts_to_disk(book_skipped_df, book_skipped_df_path, book_counts_df, book_counts_df_paths)
            book_counts_df_length = book_counts_df.shape[0]
            print(f'{book_counts_df_length} records appended to book_counts json files.')            
    else:
        # Prepbatch queue has messages (bookset queries) in it. 
        # Time to query twitter with the given bookset queries and store results. 
        # Once the queue is empty, publish to the topic 'batchbook.fifo'
        counts_df, skipped_df, has_run_out = query_twitter_total_count(prepbatch)
        counts_df_paths = [bookset_counts_all_path, bookset_counts_most_recent_path]
        write_counts_to_disk(skipped_df, bookset_skipped_df_path, counts_df, counts_df_paths)
        counts_df_length = counts_df.shape[0]
        print(f'{counts_df_length} records appended to bookset_counts json files.')
        if has_run_out and batchbook.size == 0:
            sns_publish_to_batchbook()
            
    return f'{counts_df_length} records appended to bookset_counts json files. {book_counts_df_length} records appended to book_counts json files. SNS failed to publish {SNS_FAIL_COUNT} records to batchbook.'
    
def sns_publish_to_batchbook():
    '''
    Get top 100 booksets by mention count, form individual book queries, publish to the appropriate queue.
    '''
    all_booksets_df = wr.s3.read_json(path=f's3://{BUCKET}/data/extracted/twitter/bookset_counts/most_recent')
    top_booksets_df = all_booksets_df.nlargest(300,'total_count')
    book_queries = explode_query(top_booksets_df['request_url'].tolist())
    
    sns_batches = get_sns_batches(book_queries)
    SNS_FAIL_COUNT = sns_publish(SNS, sns_batches, 'batchbook.fifo')
    wr.s3.delete_objects(f's3://{BUCKET}/data/extracted/twitter/bookset_counts/most_recent')
    return None

def query_twitter_total_count(queue):
    '''
    Read query messages from an SQS queue and queries Twitter. 
    Return number of mentions in the last 7 days using the recent counts api.
    Keep track of the 7-day total count and start and end timestamps for each query.
    '''
    sns = boto3.client('sns')
    column_names = ['request_url','start_date','end_date','total_count']
    counts_df = pd.DataFrame(columns=column_names)
    skipped_list = []
    skipped_df = pd.DataFrame(columns=['query'])
    has_run_out = False
    for i in range(10):
        msgs = SQS.receive_message(QueueUrl=queue.url, MaxNumberOfMessages=10)
        if 'Messages' in msgs:
            for msg in msgs['Messages']:
                receipt_handle = msg['ReceiptHandle']
                message = msg['Body']
                headers = {'Accept': 'application/json','Authorization': f'Bearer {TWITTER_BEARER}'} # send request to twitter
                if message[:5] != 'https':
                    message_json = json.loads(message)
                    message = message_json['Message']
                r = requests.get(url=message, headers=headers).json()
                if 'title' in r:
                    if r['title']=='Too Many Requests':
                        time.sleep(15)                                           # slow down
                    else:
                        skipped_list.append(message)                            # skip if the issue is not 'too many requests'
                        print(f're-published: {message}')
                elif 'meta' in r: 
                    total_count = r['meta']['total_tweet_count']                # total tweet counts for the past 7 days
                    start_date = r['data'][0]['start']
                    end_date = r['data'][-1]['end']
                    counts = pd.DataFrame({'request_url':message, 'start_date':start_date, 'end_date': end_date, 'total_count':total_count}, index=[0])
                    counts_df = counts_df.append(counts)
                    delete_msg_from_queue(sqs=SQS, queue=queue, receipt_handle=receipt_handle)
                    time.sleep(0.5)
        else:
            has_run_out=True
    skipped_df['query']=skipped_list
    counts_df = counts_df.reset_index().drop(columns="index")
    return counts_df, skipped_df, has_run_out
    
def write_counts_to_disk(skipped_df, skipped_df_path, counts_df, counts_df_paths):
    '''
    Writes twitter counts and list of skipped queries to json files on S3.
    '''
    for path in counts_df_paths:
        wr.s3.to_json(df=counts_df,path=path, dataset=True)
    if not skipped_df.empty:
        skipped_df.reset_index()
        wr.s3.to_json(df=skipped_df,path=skipped_df_path, dataset=True)
