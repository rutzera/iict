# https://developer.corsano.com/rest/intro/ as cs

# To do:
# - deal with wrong login (no json response) -> Can you display HTML in streamlit?
# - deal with error {'error': 'Unauthenticated.'}

# add tools path
import datetime
import streamlit as st
from tools import corsano as cs
import pandas as pd

uapi = cs.UsersAPI()
hapi = cs.HealthAPI()

st.set_page_config(layout="wide")
st.title('Your last month with Corsano')

loginplace = st.empty()
if uapi.token is None:
    with loginplace.container():
        email = st.text_input('Enter your email')
        password = st.text_input('Enter your password', type='password')
        if st.button("Use Samuel's login instead..."):
            email = 'wehs@zhaw.ch'
            password = 'Study75!'

if email and password:
    res = uapi.login(email, password)
    if uapi.token is None:
        loginplace.error('Login failed')

if uapi.token:
    loginplace.success('Login successful')
    hapi.login(uapi.token)

if hapi.token:

    today = datetime.date.today()
    start = today - datetime.timedelta(days=30)

    res = hapi.user_summaries(start.isoformat(), today.isoformat(), include_slots=0,per_page=100)
    data_df = pd.DataFrame(res['data'])
    act_df = pd.DataFrame(data_df['activity'].values.tolist())
    hr_df = pd.DataFrame(data_df['heart_rate'].values.tolist())

    st.header('Steps')
    st.bar_chart(x='date', y='total_steps', data=act_df, use_container_width=True)

    st.header('Heart Rate')
    cols = ['avg_daily_heart_rate','max_daily_heart_rate','rest_daily_heart_rate']
    st.line_chart(hr_df.set_index('date')[cols])

