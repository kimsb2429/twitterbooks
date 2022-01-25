import streamlit as st
import numpy as np
import pandas as pd
import awswrangler as wr

st.title('TWITTERBOOKS')
st.header('Week of January 22, 2022')
# st.header('week of January 22, 2022')
df = wr.s3.read_json(path='s3://warcbooks/data/staging/batch/topbooks/topbooks.json', dtype=False)
st.dataframe(df,800,1200)
# if st.checkbox('Show dataframe'):
#    chart_data = df
#    #  chart_data = pd.DataFrame(
#    #     np.random.randn(20, 3),
#    #     columns=['a', 'b', 'c'])

#    chart_data