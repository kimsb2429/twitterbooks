import numpy as np
import pandas as pd
import awswrangler as wr
import streamlit as st
import plotly
import plotly.figure_factory as ff
import datetime

# wide mode
st.set_page_config(layout="wide")

# check which week
key = 's3://warcbooks/data/staging/batch/topbooks/topbooks.json'
last_modified = wr.s3.describe_objects(key)[key]['LastModified']
collection_date = last_modified - datetime.timedelta(8)
week_of = f"Week of {collection_date.strftime('%B %-d, %Y')}"

# titles
st.title('TWITTERBOOKS')
st.header(week_of)

# two-column layout
col1, col1x, col2, = st.columns([1.5,0.05, 1])
df = wr.s3.read_json(path='s3://warcbooks/data/staging/batch/topbooks/topbooks.json', dtype=False)
years = df['year'].astype(int).tolist()
min_year = min(years)
max_year = datetime.date.today().year
np_years = np.array(years)
# min_default = int(np.quantile(np_years,0.25))
# max_default = int(np.quantile(np_years,0.75))

# slider to select year range
values = col2.slider(
     'Filter by years',
     min_year, max_year, (min_year,max_year))
col2.text("")


# the main data table
df['year'] = df['year'].astype(int)
tabledf = df[(df['year']<=values[1]) & (df['year']>=values[0])]
col1.dataframe(tabledf,height=750)

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

