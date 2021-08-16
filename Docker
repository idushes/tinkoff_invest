FROM python:3.9
WORKDIR /app
COPY app .
COPY requirements.txt .
RUN pip install psycopg2
RUN pip install -r requirements.txt
CMD python3 main.py