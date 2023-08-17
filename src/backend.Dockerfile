FROM python:3.11.4

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY start.sh /usr/start.sh
RUN chmod +x /usr/start.sh
CMD ["/usr/start.sh"]

