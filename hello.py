import streamlit as st
import numpy as np
import pandas as pd
import awswrangler as wr

df = wr.s3.read_json(path='s3://warcbooks/data/staging/batch/topbooks/topbooks.json', dtype=False)


if st.checkbox('Show dataframe'):
   chart_data = df
   #  chart_data = pd.DataFrame(
   #     np.random.randn(20, 3),
   #     columns=['a', 'b', 'c'])

   chart_data