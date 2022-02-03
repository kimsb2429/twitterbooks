import json
import pandas as pd
import awswrangler as wr
import datetime
import boto3
from lib import *

def lambda_handler(event, context):
    '''
    Send alert if prebatch and batchbook queues are not empty.
    Select relevant data to be served and copy to main directory.
    '''
    # Send alert if prebatch and batchbook queues are not empty
    sqs = boto3.client('sqs')
    prepbatch = SqsQueue(sqs=sqs, queue_name='prepbatch.fifo')
    batchbook = SqsQueue(sqs=sqs, queue_name='batchbook.fifo')
    if not ((prepbatch.size == 0) and (batchbook.size == 0)):
        conf = ConfigFromS3('warcbooks', 'script/config/hb.cfg', 'us-east-1').config 
        email = conf.get('Email','Email')
        ses = boto3.client('ses')
        response = ses.send_email(
            Source=email,
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': 'Twitterbooks: Queues Are Not Empty'},
                'Body': {'Text': {'Data': 'Either prepbatch or batchbook queue has not yet been emptied.'}}
            }
        )
        
    # read new set of counts to athena
    copy_to_csv()
    glue = boto3.client('glue')
    glue_response = glue.start_crawler(Name='book_counts') 
    
    # Combine most-mentioned books fact data with the book dimension data
    top_df = wr.s3.read_json(path='s3://warcbooks/data/extracted/twitter/topbooks/most_recent',dtype=False)
    isbn_df = wr.s3.read_json(path='s3://warcbooks/data/transformed/isbn/cur_version', dtype=False)
    isbn_df = isbn_df.drop_duplicates()
    wr.s3.to_json(df=isbn_df, path='s3://warcbooks/data/main/batch/isbn/cur_version/isbn.json')
    top_df['query']=top_df['request_url'].str.replace('https:\/\/api.twitter.com\/2\/tweets\/counts\/recent\?query=','',regex=True)
    joined_df = top_df.set_index('query').join(isbn_df.set_index('query'))
    joined_df = joined_df[pd.notna(joined_df['title'])]
    joined_df = joined_df.sort_values(by=['total_count'], ascending=False).reset_index().drop(columns=['index'])
    wr.s3.to_json(df=joined_df, path='s3://warcbooks/data/transformed/topbooks/all', dataset=True)
    wr.s3.to_json(df=joined_df, path='s3://warcbooks/data/transformed/topbooks/most_recent/topbooks.json')
    
    # Select relevant data to be served and copy to main directory
    joined_df = joined_df[['title','title_short', 'authors','date_published','total_count','query']].reset_index()
    joined_df.index = joined_df.index+1
    joined_df['date_published']=joined_df['date_published'].str[:4]
    joined_df['author_list']=joined_df['authors'].str.split(', ')
    authors=[]
    for name in joined_df['author_list']:
        if len(name)>1:
            authors.append(name[1] + " " + name[0])
        else:
            authors.append(name[0])
    joined_df['author(s)']=authors
    joined_df['author(s)']=joined_df['author(s)'].str.strip()
    joined_df = joined_df.drop(columns=['author_list','authors'])
    temp_df = joined_df.rename(columns={'date_published':'year','total_count':'mentions'})
    
    # Update the book's publication year if an earlier edition exists, using an external list
    # (The external list is downloaded from https://thegreatestbooks.org/lists/details -> Misc
    #  and uploaded to s3 manually; only needs to be done once.)
    ext_df = wr.s3.read_json('s3://warcbooks/data/extracted/bestbooks/bestbooks.json',dtype=False)
    ext_df = ext_df.rename(columns={'title':'title_temp'})
    ext_df['title_temp']=ext_df['title_temp'].str.replace('\W',' ',regex=True)
    temp_df['title_temp']=temp_df['title_short'].str.replace('\W',' ',regex=True)
    jdf = temp_df.set_index(['title_temp','author(s)']).join(ext_df.set_index(['title_temp','author(s)']), lsuffix='_left', rsuffix='_right')
    jdf['year'] = jdf['year_right'].combine_first(jdf['year_left'])
    jdf = jdf.drop(columns=['year_left','year_right']).reset_index()
    jdf = jdf[pd.notna(jdf['mentions']) & pd.notna(jdf['year'])]
    jdf = jdf.astype({"query": str, "title": str, "title_short": str, "title_temp": str, "author(s)": str, "mentions":int, "year": int})
    jdf = jdf.rename(columns={'title_short':'shortened_title'})
    jdf = jdf[['shortened_title','author(s)','year','mentions','query']]
    jdf.loc[jdf['shortened_title']=='1984', ['year']]=1949 # Special case for Orwell's Nineteen Eighty-Four, which was republished as 1984
    searchfor = ['Disney', 'Marvel','Atlus','Capcom','EA DICE','. HBO','Aesop','Gandhi','Bruce Springsteen','John Lennon','Kobe Bryant','George Lucas','Muhammad Ali','George Soros','Albert Einstein','John Paul II','From Software','Thich Nhat Hanh','Martin Luther King','Kansas','Rich Little']
    jdf = jdf[~jdf['author(s)'].str.contains('|'.join(searchfor),regex=True)]
    jdf['shortened_title'] = jdf['shortened_title'].str.replace('\(.*?\)','')
    jdf['shortened_title'] = jdf['shortened_title'].str.strip()
    jdf = jdf[~jdf['shortened_title'].str.fullmatch('It')]
    jdf = jdf[~jdf['author(s)'].str.contains('|'.join(searchfor),regex=True)]
    adf = jdf['author(s)'].str.split(expand = True).add_prefix('A')
    jdf = jdf.join(adf)
    jdf['A1']=jdf['A2'].combine_first(jdf['A1'])
    print(jdf[jdf['shortened_title'].str.contains('Jane Eyre')])
    jdf = jdf.drop_duplicates(subset=['shortened_title','A0']).reset_index(drop=True)
    jdf = jdf.drop_duplicates(subset=['shortened_title','A1']).reset_index(drop=True)
    print(jdf[jdf['shortened_title'].str.contains('Jane Eyre')])
    jdf = jdf.drop(columns=['A0','A1','A2'])
    jdf = jdf.nlargest(100,'mentions').reset_index().drop(columns=['index'])
    jdf.index= jdf.index+1
    
    # Write to main most_recent
    wr.s3.to_json(df=jdf, path='s3://warcbooks/data/main/batch/topbooks/most_recent/topbooks.json')
    
    # Write to main all
    datestr = datetime.now().strftime('%Y%m%d%H%M%S')
    wr.s3.to_json(df=jdf, path=f's3://warcbooks/data/main/batch/topbooks/all/{datestr}.json')
    
    return f'{jdf.shape[0]} records were written to main/batch/topbook directories.'
    
def copy_to_csv():
    '''
    Copies json files to csv for athena glue crawler
    '''
    counts_df = wr.s3.read_json(path='s3://warcbooks/data/extracted/twitter/book_counts/most_recent/',dtype=False)\
                .sort_values('start_date',ascending=False)\
                .drop_duplicates(subset='request_url',keep='first')\
                .reset_index(drop=True)\
                .set_index(['start_date','end_date','request_url'])
    datestr = datetime.now().strftime('%Y%m%d%H%M%S')
    wr.s3.to_csv(df=counts_df, path=f's3://warcbooks/data/extracted/twitter/book_counts/csv/batch_copied_from_json.csv')