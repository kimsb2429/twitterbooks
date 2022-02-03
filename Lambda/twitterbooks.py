import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime
from lib import *
    
def lambda_handler(event, context):
    '''
    Lambda handler for extracting from Common Crawl and ISBNDB.
    Extracted ISBN data from Common Crawl is stored on S3://warcbooks/data/extracted/parquet and queryable through Athena.
    Extracted book data from ISBNDB is stored on S3://warcbooks/data/extracted/json.
    A master copy of all book data from ISBND is stored on S3://warcbooks/data/extracted/isbn/master.
    '''
    # version control
    version = 'cur_version'
    
    # date string to be used throughout
    datestr = datetime.now().strftime('%Y%m%d%H%M%S')
    
    # aws parameters and boto3 clients
    bucket = 'warcbooks'                                                                  
    region = 'us-east-1'
    topic_name = 'prepbatch.fifo'
    s3 = boto3.client('s3')                                                               
    s3_resource = boto3.resource('s3')
    athena = boto3.client('athena')
    sns = boto3.client('sns')
    
    # api tokens from config file on s3
    conf = ConfigFromS3(bucket, 'script/config/hb.cfg', region).config                    
    ISBN_TOKEN = conf.get('ISBNDB','Token')                                               
    TWITTER_BEARER = conf.get('Twitter','Bearer')
    
    # Get latest crawl name from common crawl
    crawl = get_latest_common_crawl('https://index.commoncrawl.org/collinfo.json')        
    
    # s3 key abbreviations
    key = 'data/extracted'
    warc_key = f'{key}/warc'                                                              
    warc_key_json = f'{warc_key}/json/{crawl}/{datestr}'
    warc_key_parquet = f'{warc_key}/parquet/{crawl}/{datestr}'
    
     # athena sql parameters
    catalog = 'ccindex'                                                                  
    db = 'ccindex'
    table = 'books_' + datestr
    columns = 'url, warc_filename, warc_record_offset, warc_record_length'
    filter_column = 'url'
    regex_filter = "'^https:\/\/www.amazon.com\/[^\/]*\/dp\/(0|1)[0-9]{9}\/.*'"
    
    # in case sns_publish doesn't run properly
    sns_fail_count = -1
    
    try:
        # Parse ISBNs from Amazon URLs in the Common Crawl archives
        drop_query = query_builder('DROP', db, table)                                   
        create_query = query_builder('CREATE', db, table, catalog, columns, bucket, warc_key_parquet, 'PARQUET',\
            where=f"crawl = '{crawl}' AND length(regexp_extract({filter_column}, {regex_filter}))>0")
        unload_query = query_builder('UNLOAD', db, table, bucket=bucket, key=warc_key_json, formatting='JSON',\
            select="SPLIT(url,'/')[6] AS ISBN")
    
        # Parse Common Crawl's amazon book urls to get ISBNs, use athena to store as json file
        athena_start_query_execution(athena,drop_query,f's3://{bucket}/{warc_key}/drop_output/{crawl}/{datestr}')               
        athena_start_query_execution(athena,create_query,f's3://{bucket}/{warc_key}/create_output/{crawl}/{datestr}')
        athena_start_query_execution(athena,unload_query,f's3://{bucket}/{warc_key}/unload_output/{crawl}/{datestr}')          
        
        # Query ISBNDB API with the ISBNs for book data
        b = s3_resource.Bucket(bucket)                                                        
        df = pd.DataFrame()
        for obj in b.objects.filter(Prefix='data'):
            if obj.key.startswith(warc_key_json) and obj.key.endswith('.gz'):
                new_df = pd.read_json(obj.get()['Body'], compression='gzip', lines=True, dtype=False)
                df = df.append(new_df)
        df = df.drop_duplicates()                                                                      # drop duplicates
        booksdf = request_ISBNDB(df, 'https://api2.isbndb.com/books', ISBN_TOKEN, chunk_length = 1000) # request book data from ISBNDB
        booksdf = booksdf.reset_index().drop(columns='index')                                          # index must be unique
        wr.s3.to_json(df=booksdf, path=f's3://{bucket}/{key}/isbn/{version}/{datestr}.json') # write to S3 as json files      
    
        # Transform and store
        trans_df = transform_isbn(booksdf)
        wr.s3.to_json(df=trans_df, path=f's3://{bucket}/data/transformed/isbn/{version}/{datestr}.json')
    
    except:
        pass  # If Common Crawl API throws a "Please reduce your rate" exception, work with existing master book data
    
    booksdf = wr.s3.read_json(f's3://warcbooks/data/extracted/isbn/cur_version')
    trans_df = transform_isbn(booksdf)
    wr.s3.to_json(df=trans_df, path=f's3://{bucket}/data/transformed/isbn/{version}/{datestr}.json')
    
    
    # Regardless of whether queries to ISBN ran successfully, read all transformed data
    tdf = wr.s3.read_json(f's3://{bucket}/data/transformed/isbn/{version}', dtype=False)
    
    # Drop duplicates if two twitter queries are the same, keep first
    tdf = tdf.drop_duplicates(subset='query').drop(columns=['index']).reset_index().drop(columns=['index'])
    
    # Pack 10ish books into each query to reduce the number of queries to Twitter API
    queries = build_tweet_counts_query(tdf['query'])
    
    # Publish list of chunked queries via SNS to the appropriate topic
    sns_batches = get_sns_batches(queries)
    sns_fail_count = sns_publish(sns, sns_batches, topic_name)
    
    # Empty last run's most_recent folders to prep for the next lambda function
    try:
        wr.s3.delete_objects('s3://warcbooks/data/extracted/twitter/topbooks/most_recent')
        wr.s3.delete_objects('s3://warcbooks/data/extracted/twitter/book_counts/most_recent')
    except Exception as e:
        print(e)
        
    return f'{tdf.shape[0]} isbn records were added. SNS failed to publish {sns_fail_count} messages.'

def transform_isbn(tdf):
    '''
    Transforms ISBNDB data to be used in twitter queries.
    Input: pandas dataframe
    Output: pandas dataframe
    '''
    # Choose columns of interest
    tdf = tdf[['publisher','title','pages','date_published','authors','isbn','image','binding']]
    
    # Handle authors
    tdf = tdf[tdf['authors'].apply(lambda x: isinstance(x, list))]       # discard rows if data type of 'authors' != list
    tdf['authors'] = [' '.join(l) for l in tdf.authors.tolist()]         # convert 'authors' to string, remove comma
    tdf['authors'] = tdf['authors'].str.strip()
    tdf = tdf[tdf['authors']!='']                                        # discard rows if 'authors' is blank
    tdf = tdf.drop_duplicates()
    
    # Handle titles
    tdf['title_short'] = [title.split(':')[0] for title in tdf['title']] # title_short = first part of title (drop ": A Novel")
    
    # Build query from author and title
    tdf['query'] = tdf[['title_short','authors']].agg(' '.join, axis=1)  # combine title and author into query string
    tdf['query'] = [q.lower() for q in tdf['query']]

    # Remove unwanted words and characters from query, format it for Twitter
    rm_words = ['a','an','the','dr','mr','mrs','prof','msgr','rev','rt','sr','jr','phd','lcsw','esq']
    rm_chars = ['\(.*?\)','\<.*?\>']
    tdf = remove_regex(tdf,'query',rm_words,rm_chars,code_space=True, add_paren=True)
    
    # Clean up
    tdf = tdf.reset_index()                                     
    tdf['authors'] = tdf['authors'].str.strip() 
    tdf = tdf[tdf['authors']!='']                                    
    tdf = tdf[pd.notna(tdf['pages'])]                                # has page count
    tdf = tdf[pd.notnull(tdf['date_published'])]                     # has pub date
    
    # More strict requirements:
    # Must have more than 2 unique words in query
    wordlists = [q[1:-1].split('%20') for q in tdf['query'].tolist()]             
    no_good_wordlists = [wordlist for wordlist in wordlists if len(set(wordlist))<3]
    no_good_queries = ['%20'.join(wordlist) for wordlist in no_good_wordlists]
    no_good_queries = [f'({query})' for query in no_good_queries]
    tdf = tdf[~tdf['query'].isin(no_good_queries)]
    
    # Remove query if a phrase is repeated with no other words, e.g. 'taste of home of home taste'
    # Not taking set(query) because redundancy is useful for further tuning
    qlist = []
    for q in tdf['query']:
        is_double = True
        qfull = q[1:-1].split('%20')
        qset = set(q[1:-1].split('%20'))
        if len(qset) * 2 == len(qfull):
            qdf = pd.DataFrame()
            qdf['q']=qfull
            vc = qdf.value_counts()
            for count in vc:
                if count != 2:
                    is_double = False
                    break
            if is_double == True:
                qlist.append(q)
    tdf = tdf[~tdf['query'].isin(qlist)]
    
    # Remove query if it's just the author name
    # Sort words in queries to catch more duplicates
    authorsplit = [a.lower().replace(',','').replace('.','').split(' ') for a in tdf['authors']]
    authorset = [set(a) for a in authorsplit]
    tdf['authorsplit']=authorsplit
    tdf['authorset']=authorset
    querysplit = [q[1:-1].split('%20') for q in tdf['query']]
    queryset = [set(q) for q in querysplit]
    tdf['querysplit']=querysplit
    tdf['queryset']=queryset
    tdf['querysplit'] = [sorted(q) for q in tdf['querysplit']]      # sort words in query
    tdf['query']=tdf['querysplit'].str.join('%20')
    tdf['query']=['('+q+')' for q in tdf['query']] 
    tdf = tdf.drop_duplicates(subset=['query'])                     # drop duplicates on sorted query
    tdf = tdf[tdf['queryset']!=tdf['authorset']]                    # drop queries that are only author names
    tdf = tdf.drop(columns=['authorset','queryset','authorsplit','querysplit'])
    
    return tdf