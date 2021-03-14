FROM python:3.7
WORKDIR /app


COPY requirements.txt /app
RUN pip install -r /app/requirements.txt

COPY config.py /app
COPY utils.py /app
COPY preprocess_trade_data.py /app
COPY calc_earning_at_risk.py /app
COPY calc_statistics.py /app
COPY main.py /app

USER nobody

ENTRYPOINT ["python", "main.py"]