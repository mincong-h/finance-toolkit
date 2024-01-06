FROM python:3.8
WORKDIR /app/finance-toolkit/
COPY . /app/finance-toolkit/
RUN python setup.py install

VOLUME ["/data/source", "/data/target"]
ENV DOWNLOAD_DIR=/data/source
ENV FINANCE_ROOT=/data/target

ENTRYPOINT ["/app/finance-toolkit/entrypoint.sh"]
