import numpy as np
import pandas as pd
import awswrangler as wr
import streamlit as st
import plotly
import plotly.figure_factory as ff
import datetime
import random
import yaml
import requests
from twitter_stream import RecentSearch
import streamlit.components.v1 as components


# wide mode
st.set_page_config(layout="wide")

# one-time computes
@st.cache
def caching():
   # check which week
   key = 's3://warcbooks/data/main/batch/topbooks/most_recent/topbooks.json'
   last_modified = wr.s3.describe_objects(key)[key]['LastModified']
   collection_date = last_modified - datetime.timedelta(8)
   week_of = collection_date.strftime('%B %-d, %Y')

   # get top-books df
   df = wr.s3.read_json(path=key, dtype=False)

   # get num books processed
   booksdf = wr.s3.read_json(path='s3://warcbooks/data/main/batch/isbn/cur_version', dtype=False)
   num_books = booksdf.shape[0]
   return week_of, df, num_books

week_of, df, num_books = caching()

# tweet stream class and functions
class Stream(RecentSearch):
    max_results = ['10']
    expansions = ['author_id']
    tweet_fields = ['created_at']
    user_fields = ['name']
    def set_query(self, query):
        Stream.query = query

@st.cache
def tweets(qlist):
   stream = Stream() 
   columns = ['id','text','created_at','author_id','username']
   tdf = pd.DataFrame(columns=columns) 
   try:
      for q in qlist:
         stream.set_query([q])
         for tweet in stream.connect():
               if 'data' in tweet:
                  for user in tweet['includes']['users']:
                     if user['id'] == tweet['data'][0]['author_id']:
                           username = user['username']
                  temp_df = pd.DataFrame({
                     'id':tweet['data'][0]['id'],
                     'text':tweet['data'][0]['text'],
                     'created_at':tweet['data'][0]['created_at'],
                     'author_id':tweet['data'][0]['author_id'],
                     'username':username
                  }, index=[0])
                  tdf = tdf.append(temp_df)
                  break
   except Exception as e:
      print(e)
   tdf['created']=pd.to_datetime(tdf['created_at'],format='%Y-%m-%dT%H:%M:%S.%fZ')
   tdf = tdf.sort_values(by=['created'],ascending=False)
   return tdf

@st.cache
def get_queries(df):
   tdf = df[['query']]
   tdf['tquery'] = tdf['query'].str.replace('%20',' ', regex=False)
   tdf['tquery'] = tdf['tquery'].str.replace('(','',regex=False)
   tdf['tquery'] = tdf['tquery'].str.replace(')','',regex=False)
   qlist = tdf['tquery'].tolist()
   tdf = tdf.drop(columns=['tquery'])
   qlist = [q+' -is:retweet -is:reply lang:en' for q in qlist]
   return qlist

@st.cache
def get_pretty_tweets():
# def get_pretty_tweets(refresh=datetime.datetime.utcfromtimestamp(1284286794)):
#    refresh=refresh
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
   with open("../.twitter-keys.yaml", 'r') as stream:
      tkeys = yaml.safe_load(stream)
   for i,tid in enumerate(tids):
      tusername = tusernames[i]
      url = f'https://publish.twitter.com/oembed?url=https%3A%2F%2Ftwitter.com%2F{tusername}%2Fstatus%2F{tid}'
      headers = {'Accept': 'application/json','Authorization': f"Bearer {tkeys['keys']['bearer_token']}"} # send request to twitter
      resp = requests.get(url=url, headers=headers).json()
      hstr += resp['html']
   hstr = hstr.replace('blockquote class','blockquote data-theme="dark" class')
   return hstr

# titles
st.title('TWITTERBOOKS')

# two-column layout
col1, col1x, col2, = st.columns([1.4,0.05, 1.1])
col1.header(f'The Hot 100 — Week of {week_of}')
col2.header('{:,} books queried'.format(num_books))
years = df['year'].astype(int).tolist()
min_year = min(years)
max_year = datetime.date.today().year
np_years = np.array(years)
# min_default = int(np.quantile(np_years,0.25))
# max_default = int(np.quantile(np_years,0.75))

# slider to select year range
values = col2.slider(
     'Filter by publication year',
     min_year, max_year, (min_year,max_year))

# the main data table
df['year'] = df['year'].astype(int)
tabledf = df[['shortened_title','author(s)','year','mentions']]
tabledf = tabledf[(tabledf['year']<=values[1]) & (tabledf['year']>=values[0])]
col1.dataframe(tabledf, height=1350)
# my_table = col1.table(tabledf)

# tweets or stats
chosen = col2.radio(
   'View',
   ("Tweets", "Stats"))

if chosen=='Tweets':
   with col2:
      try:
         html_str = get_pretty_tweets()
         # refresh_button = st.button('Get More Tweets')
         # if refresh_button == True:
         #    html_str = get_pretty_tweets(refresh=datetime.datetime.now())
         components.html(html_str, height=1100,scrolling=True)
      except Exception as e:
         print(e)

if chosen=='Stats':
   # group mention counts by year and by the slider selection
   yeardf = df[['year','mentions']].groupby(['year']).sum()
   yeardf = yeardf.reset_index()
   yeardf['year'] = yeardf['year'].astype(int)
   group = []
   for row in yeardf['year']:
      if row<values[0]:
         group.append('pre')
      elif (row>=values[0]) & (row<=values[1]):
         group.append('mid')
      else:
         group.append('post')
   yeardf['group']=group

   # barchart 1
   mid = yeardf[yeardf['group']=='mid']
   mid_indexed = mid.groupby(['year']).sum()
   col2.bar_chart(mid_indexed)

   # barchart 2
   pmp = yeardf.groupby('group').mentions.transform(np.sum).drop_duplicates().tolist()
   if len(pmp)==1:
      year_range = [f'{min_year} - {max_year}']
   elif len(pmp)==2:
      if min_year == values[0]:
         year_range = [f'{min_year} - {str(values[1])}', f'{str(values[1]+1)} - {max_year}']   
      else:
         year_range = [f'{str(values[0])} - {str(values[1]-1)}', f'{str(values[1])} - {max_year}']   
   else:
      year_range = [f'{min_year} - {str(values[0]-1)}', f'{str(values[0])} - {str(values[1])}', f'{str(values[1]+1)} - {max_year}']
   pre_mid_post = pd.DataFrame()
   pre_mid_post['year_range'] = year_range
   pre_mid_post['mentions'] = [x/sum(pmp) for x in pmp]
   pre_mid_post = pre_mid_post.groupby(['year_range']).sum()
   col2.bar_chart(pre_mid_post)

for i in range(12):
   st.write('')
st.write('***')
col1, col1x, col2, = st.columns([1.4,0.05, 1.1])
col1.text('Author names and shortened titles are used to search for tweets.\n\
   For example, the search criteria for "The Waves" by Virginia Woolf is:\n\
   any tweet that includes the words "waves", "virginia", and "woolf".\n')
col1.text('Random 10 books are selected to display the tweets.\n\
Results are updated on a weekly basis.')
col2.text('Further discussion on:')
col2.write('[github link](https://github.com/kimsb2429/twitterbooks)')