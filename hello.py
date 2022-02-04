import numpy as np
import pandas as pd
import awswrangler as wr
import streamlit as st
import datetime
import time
import random
import requests
import streamlit.components.v1 as components
import altair as alt
import json
import boto3

# wide mode
st.set_page_config(layout="wide")

# cache results
@st.experimental_memo(ttl=3600)
def caching():
   '''
   Read twitter API results and number of books queried from S3
   '''
   # twitter api results
   key = 's3://warcbooks/data/main/batch/topbooks/most_recent/topbooks.json'
   df = wr.s3.read_json(path=key, dtype=False)

   # number of books queried
   booksdf = wr.s3.read_json(path='s3://warcbooks/data/main/batch/isbn/cur_version', dtype=False)
   num_books = booksdf.shape[0]
   return df, num_books

def tweets(qlist):
   '''
   Get recent tweets of the books on the list.
   Input: list of book queries
   '''
   columns = ['id','text','created_at','author_id','username']
   tdf = pd.DataFrame(columns=columns) 
   try:
      for q in qlist:
         # specify response fields
         url=f'https://api.twitter.com/2/tweets/search/recent?query={q}&max_results=10&expansions=author_id&user.fields=username&tweet.fields=created_at'
         headers = {'Accept': 'application/json','Authorization': f"Bearer {st.secrets.t_bearer_token}"} # send request to twitter
         
         # request and parse
         r = requests.get(url=url, headers=headers).json()
         if 'data' in r:
            for user in r['includes']['users']:
               if user['id'] == r['data'][0]['author_id']:
                  username = user['username']
            temp_df = pd.DataFrame({
               'id':r['data'][0]['id'],
               'text':r['data'][0]['text'],
               'created_at':r['data'][0]['created_at'],
               'author_id':r['data'][0]['author_id'],
               'username':username
            }, index=[0])
            tdf = tdf.append(temp_df)

            # keep 12 tweets
            if tdf.shape[0]>12:
               break
   except Exception as e:
      print(e)
   
   # convert and format the tweet datetime
   tdf['created']=pd.to_datetime(tdf['created_at'],format='%Y-%m-%dT%H:%M:%S.%fZ')

   # sort by tweet datetime
   tdf = tdf.sort_values(by=['created'],ascending=False)
   return tdf

def get_queries(df):
   '''
   Build twitter queries
   Input: dataframe of book mentions
   '''
   tdf = df[['query']]
   tdf['tquery'] = tdf['query'].str.replace('(','',regex=False)
   tdf['tquery'] = tdf['tquery'].str.replace(')','',regex=False)

   # filter out retweets and replies
   tdf['tquery'] = tdf['tquery'] + '%20%2Dis%3Aretweet%20%2Dis%3Areply%20lang%3Aen'
   qlist = tdf['tquery'].tolist()
   tdf = tdf.drop(columns=['tquery'])
   return qlist

@st.experimental_memo(ttl=3600)
def get_pretty_tweets(df):
   '''
   Use twitter's built-in styling for embedding tweets.
   '''
   # <script>document.documentElement.style.setProperty('color-scheme', 'dark');</script>
   # <script>document.body.style.backgroundColor = "black";</script>
   hstr = """
   <script>window.twttr = (function(d, s, id) {
   var js, fjs = d.getElementsByTagName(s)[0],
      t = window.twttr || {};
   if (d.getElementById(id)) return t;
   js = d.createElement(s);
   js.id = id;
   js.src = "https://platform.twitter.com/widgets.js";
   fjs.parentNode.insertBefore(js, fjs);

   t._e = [];
   t.ready = function(f) {
      t._e.push(f);
   };

   return t;
   }(document, "script", "twitter-wjs"));</script>
   """
   qlist = get_queries(df)
   qlist = random.sample(qlist,25)
   tdf = tweets(qlist)
   tids = tdf['id'].tolist()
   tusernames = tdf['username'].tolist()
   # my_table = col1.table(tdf)

   for i,tid in enumerate(tids):
      tusername = tusernames[i]
      url = f'https://publish.twitter.com/oembed?url=https%3A%2F%2Ftwitter.com%2F{tusername}%2Fstatus%2F{tid}'
      headers = {'Accept': 'application/json','Authorization': f"Bearer {st.secrets.t_bearer_token}"} # send request to twitter
      resp = requests.get(url=url, headers=headers).json()
      hstr += resp['html']
   hstr = hstr.replace('blockquote class','blockquote data-theme="dark" class')
   return hstr

# singleton cache
@st.experimental_singleton(suppress_st_warning=True)
def build_sessiondf(df,rebuild=datetime.date(1900,1,1)):
   '''
   Build dataframe to be used during session
   '''
   # cache can be finicky
   rebuild = rebuild

   # use session key-value pairs to remember the S3 dataframe
   for row in df.itertuples():
      k = row[0]
      v = (row[1],row[2],row[3],row[4],row[5])
      # use session variable to control when to rebuild session dataframe from the S3 dataframe
      if (k not in st.session_state) or (rebuild!=datetime.date(1900,1,1)):
         st.session_state[k] = v

   # build session dataframe
   sessiondf = pd.DataFrame(columns=['shortened_title','author(s)','year','mentions','query'])
   for key in st.session_state.keys():
      if key != 'title' and key != 'update' and key != 'author' and key != 'update_ind' and key != 'auto_rerun':
         print(st.session_state[key])
         temp_df = pd.DataFrame({'shortened_title':st.session_state[key][0],\
                  'author(s)':st.session_state[key][1],\
                  'year':st.session_state[key][2],\
                  'mentions':st.session_state[key][3],\
                  'query':st.session_state[key][4]},index=[0])
         sessiondf = sessiondf.append(temp_df)
   
   # sort by mentions and adjust index to start at 1
   sessiondf = sessiondf.sort_values(by='mentions',ascending=False).reset_index(drop=True)
   sessiondf.index = sessiondf.index+1
   sessiondf = sessiondf[['shortened_title','author(s)','year','mentions','query']]
   return sessiondf

def color_new_mention(s):
   '''
   Mapping function for highlighting the updated row.
   '''
   if st.session_state['title'] != '-1':
      return ['background-color: green']*len(s)\
          if s['shortened_title']==st.session_state['title'] and s['author(s)']==st.session_state['author'] else ['background-color: #0f1116']*len(s)      
   else:
      return ['background-color: #0f1116']*len(s)

def main():

   # cache data from S3
   df, num_books = caching()

   # distinguish user refresh and count update
   # singleton cache can be finicky, so some redundancy here
   rebuild = datetime.date(1900,1,1)
   if 'auto_rerun' not in st.session_state:
      st.session_state['auto_rerun'] = False
   if st.session_state['auto_rerun'] == False:
      st.experimental_singleton.clear()
      rebuild = datetime.date.today()
      for key in st.session_state.keys():
         del st.session_state[key] 
   st.session_state['auto_rerun'] = False

   # other session variables
   if 'title' not in st.session_state:
      st.session_state['title'] = '-1'
   if 'author' not in st.session_state:
      st.session_state['author'] = '-1'
   if 'update' not in st.session_state:
      st.session_state['update'] = 'Listening for an update . . .'

   # title
   st.title('TWITTERBOOKS')

   # subtitle
   today = datetime.datetime.now()
   last_week = today - datetime.timedelta(7)
   st.header('100 Most Tweeted Books')
   
   # two-column layout
   col1, _, col2, = st.columns([1.3,0.05, 1.1])

   # time frame of the data
   col1.code('{} — {}, {:,} books tracked'.format(last_week.strftime('%B %-d, %Y %H:%M:%S'), today.strftime('%B %-d, %Y %H:%M:%S'), num_books))

   # build session table
   df['year'] = df['year'].astype(int)
   if 'update_ind' not in st.session_state:
      st.session_state['update_ind'] = 0
   sessiondf = build_sessiondf(df,rebuild)

   # display updates
   with col1:
      st.spinner('Listening for an update . . .')
      col1.code(st.session_state['update'])

   # build display table from session table
   dispdf = sessiondf[['shortened_title','author(s)','year','mentions']].sort_values(by='mentions',ascending=False).reset_index(drop=True)
   dispdf.index = dispdf.index + 1

   # display display table
   col1.dataframe(dispdf.style.apply(color_new_mention, axis=1), height=3000)
   
   # tweets
   with col2:
      col2.subheader("Some Tweets")
      try:
         html_str = get_pretty_tweets(df)
         components.html(html_str,height=500,scrolling=True)
      except Exception as e:
         print(e)

   # stats
   col2.text('')
   col2.subheader("Some Stats")
   col2.text('')
   col2.write('By Year')
   col2.text('')

   # chart 1: by publication year
   df_indexed = df.rename(columns={"year":"year_of_publication"})
   df_indexed = df_indexed[['year_of_publication','mentions']].groupby(['year_of_publication']).sum().reset_index()
   col2.write(alt.Chart(df_indexed).mark_bar().encode(
      x=alt.X('year_of_publication', sort=None),
      y='mentions',
   ))

   # chart 2: group mention counts by year and by quantiles
   yeardf = df[['year','mentions']].groupby(['year']).sum()
   yeardf = yeardf.reset_index()
   yeardf['year'] = yeardf['year'].astype(int)
   max_year = datetime.date.today().year
   bins = list(range(1900,max_year,25))
   bins.insert(0,-10000)
   bins.append(max_year)
   labels = [str(bin)+' - '+str(bins[i+1]) for i, bin in enumerate(bins) if i < len(bins)-1]
   yeardf['year_of_publication'] = pd.cut(yeardf['year'], bins=bins, labels=labels)
   year_range_df = yeardf.groupby(['year_of_publication']).sum().reset_index().drop(columns=['year'])
   col2.write(alt.Chart(year_range_df,width=400, height=300).mark_bar().encode(
      x=alt.X('year_of_publication', sort=None),
      y='mentions',
   ))

   col2.text('')
   col2.write('By Author')
   col2.text('')

   # chart 3 - 7: by author
   authordf = dispdf[['author(s)','mentions']].groupby('author(s)').sum().sort_values(by=['mentions'],ascending=False).reset_index()
   split_dfs = np.array_split(authordf, int(authordf.shape[0]/(min(authordf.shape[0],15))))
   for split_df in split_dfs:
      col2.write(alt.Chart(split_df,width=400).mark_bar().encode(
         x=alt.X('author(s)', sort=None),
         y='mentions',
      ))
   
   # footnotes
   for i in range(12):
      st.write('')
   st.write('***')
   col1, col1x, col2, = st.columns([1.3,0.05, 1.1])
   col1.text('Author names and shortened titles are used to search for tweets.\n\
      For example, the search criteria for "The Waves" by Virginia Woolf is:\n\
      any tweet that includes the words "waves", "virginia", and "woolf".\n')
   col2.text('Further discussion on:')
   col2.write('[github link](https://github.com/kimsb2429/twitterbooks)')

   return sessiondf

def bearer_oauth(r):
   '''Method required by bearer token authentication.'''
   r.headers["Authorization"] = f"Bearer {st.secrets.t_bearer_token}"
   r.headers["User-Agent"] = "v2SampledStreamPython"
   return r

def twitter_query_update_count(queries):
   '''
   Count mentions since the last batch job
   Input: list of query strings
   Output: dataframe of counts, the ending timestamp of last batch job
   '''
   column_names = ['request_url','start_date','end_date','tweet_count']
   counts_df = pd.DataFrame(columns=column_names)      
   athena = boto3.client('athena', region_name='us-east-1')
   output = 's3://warcbooks/data/extracted/twitter/book_counts/athena/'
   for query in queries:
      url = f'https://api.twitter.com/2/tweets/counts/recent?query={query}'

      # find out when the last batch job ended
      athena_query = f"select end_date from csv where request_url = '{url}' limit 1"
      results = athena_start_query_execution(athena,athena_query,output)
      print(results)
      last_end_date = results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']

      # send request to twitter
      response = requests.request("GET", url, auth=bearer_oauth, stream=True)
      print(response.status_code)
      if response.status_code==200:
         for response_line in response.iter_lines():
            if response_line:
               json_response = json.loads(response_line)
               if 'data' in json_response:
                  for count in json_response['data']:
                     start_date = count['start']
                     end_date = count['end']
                     tweet_count = count['tweet_count']
                     cur_count = pd.DataFrame({'request_url':url, 'start_date':start_date, 'end_date': end_date, 'tweet_count':tweet_count}, index=[0])
                     counts_df = counts_df.append(cur_count)
         response.close()
      elif response.status_code == 429:
         # if 'too many requests', do not move the cursor forward
         st.session_state['update_ind'] -= 1
         response.close()
         time.sleep(60)
      else:
         raise Exception(
            "Request returned an error: {} {}".format(
                  response.status_code, response.text
            )
         )
   return counts_df, last_end_date

def get_query_results(athena, response):
   '''Get athena query results'''
   query_results = athena.get_query_results(QueryExecutionId=response['QueryExecutionId'])
   return query_results

def wait_query_success(athena, response):
   '''Check athena query status until it returns "SUCCEEDED"'''
   status=''
   while status != 'SUCCEEDED':
      query_execution = athena.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
      status = query_execution['QueryExecution']['Status']['State']
      if status == 'QUEUED' or status == 'RUNNING':
         pass
      if status == 'FAILED' or status == 'CANCELLED':
         print(status + '\n')
         print(query_execution)
         raise Exception(query_execution)
   results = get_query_results(athena,response)
   return results

def athena_start_query_execution(athena, query, path):
   '''Execute Athena query with given query and output path and wait for success'''
   response = athena.start_query_execution(
      QueryString=query,
      ResultConfiguration={'OutputLocation': f'{path}'},
      WorkGroup='primary',
      QueryExecutionContext={'Catalog': 'AwsDataCatalog','Database': 'ccindex'}
   )
   results = wait_query_success(athena,response)
   return results

def update_count(sessiondf):
   '''
   Update the dataframe and charts with new mention counts.
   Input: session dataframe
   '''
   # cursor for the session dataframe index
   update_ind = st.session_state['update_ind']

   # restart at first row after updating last row
   if update_ind == sessiondf.shape[0]-1:
      st.session_state['update_ind'] = 1
   if update_ind > 0:
      # find query for the row
      query_list = sessiondf['query'].tolist()
      queries = [query_list[update_ind]]

      # get query results
      counts_df, last_end_date = twitter_query_update_count(queries)
      counts_df = counts_df.reset_index(drop=True)
      if counts_df.shape[0] >0:
         counts_df = counts_df.groupby('request_url').sum().reset_index()
         print(update_ind)
         print(sessiondf.head())
         author = sessiondf.at[update_ind,'author(s)']
         title = sessiondf.at[update_ind,'shortened_title']
         sessiondf.at[update_ind,'mentions'] += counts_df['tweet_count'][0]
         st.session_state[update_ind] = (title,author,sessiondf.at[update_ind,'year'],sessiondf.at[update_ind,'mentions'],sessiondf.at[update_ind,'query'])
         st.session_state['title'] = title
         st.session_state['author'] = author
         st.session_state['update'] = f"\n{counts_df['tweet_count'][0]} mentions of {title} by {author} since {last_end_date}."
   
   # cursor + 1
   st.session_state['update_ind'] += 1
   
   # slow down between updates
   time.sleep(3)

   # mark auto_rerun (not user refresh) to maintain cache
   st.session_state['auto_rerun'] = True

   # rerun from the top
   st.experimental_rerun()

if __name__ == "__main__":
   sdf = main()
   update_count(sdf)