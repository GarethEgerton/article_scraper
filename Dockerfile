FROM python:3.7

WORKDIR /nathnaconline
COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

EXPOSE 8080
COPY . /nathnaconline
CMD streamlit run visualisation.py --server.port 8080 --server.enableCORS false visualisation.py