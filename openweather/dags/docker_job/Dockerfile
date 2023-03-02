FROM apache/airflow:latest

USER root
RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get -y install zip unzip \
  && rm -rf /var/lib/apt/lists/* \
  && apt-get -fy install \
  && apt-get --fix-broken install \
  && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | tee /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update \
  && apt-get install -y xvfb \
  && apt-get install -y google-chrome-stable \
  && chmod -R 777 /opt/airflow/logs \
  && chmod -R 777 /opt/airflow/dags \
  && chmod -R 777 /opt/airflow \
  && chown -R airflow:5000 /opt/airflow/logs \
  && chown -R airflow:5000 /opt/airflow \
  && chown -R airflow:5000 /opt/airflow/dags \
  && curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

USER airflow
COPY requirements.txt requirements.txt
RUN pip install --upgrade pip \
 && pip install -r requirements.txt
#  && pip install 'apache-airflow[amazon]' \
#  && pip install 'apache-airflow-providers-amazon' \
#  && pip install 'apache-airflow-providers-microsoft-azure'
CMD ["echo", "hello world"]


# RUN apt install expect -y
# RUN apt install tor -y