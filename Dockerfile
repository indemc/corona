FROM apache/airflow:2.5.1

RUN pip install sqlalchemy
RUN pip install bs4
RUN pip install pandas
RUN pip install psycopg2
