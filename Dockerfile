FROM python:3.7
WORKDIR /app/finance-toolkit/
COPY . /app/finance-toolkit/
RUN python setup.py install

VOLUME ["/data/source", "/data/target"]
ENV FINANCE_ROOT=/data/target

ENTRYPOINT ["/app/finance-toolkit/entrypoint.sh"]
