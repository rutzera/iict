# write a streamlit which uses this library to create a chat:
# https://pypi.org/project/streamlit-chat/

from numpy import random
import streamlit as st
from streamlit_chat import message

# Add a title
st.title("Chat Demo")

# Initialization
if 'chat' not in st.session_state:
    st.session_state.chat = [{'msg': "Hi I'm the bot!", 'is_user': False}]

# write a list of bot messages which work independently of the user input
botmsgs = ['Hello', 'How are you?', 'I am fine, thanks', 'What is your name?', 'My name is Streamlit', 'Nice to meet you', 'Bye']
text = st.text_input("Enter your message")
if text:
    st.session_state.chat.append({'msg':text, 'is_user':True})
    # add random bot message
    st.session_state.chat.append({'msg':botmsgs[random.randint(0, len(botmsgs)-1)], 'is_user':False})

avatar_styles = {
    True: "Gracie",
    False: "Simon",
}

for i in range(len(st.session_state.chat)-1, -1, -1):
    el = st.session_state.chat[i]
    message(el['msg'], is_user=el['is_user'],key=str(i), avatar_style="personas",seed=avatar_styles[el['is_user']])



