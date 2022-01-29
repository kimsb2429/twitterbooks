FROM python:3.8.10
WORKDIR /Users/jaekim/twitterbooks
COPY requirements.txt ./requirements.txt
COPY hello.py ./hello.py
COPY .streamlit/secrets.toml ./.streamlit/secrets.toml
RUN python3 -m pip install -r requirements.txt
EXPOSE 8501
ENTRYPOINT [ "streamlit", "run" ]
CMD [ "hello.py" ]
