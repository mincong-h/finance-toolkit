FROM python:3.10-alpine
WORKDIR /app/finance-toolkit/
COPY . /app/finance-toolkit/
RUN pip install --upgrade setuptools pip
RUN python setup.py install

VOLUME ["/data/source", "/data/target"]
ENV DOWNLOAD_DIR=/data/source
ENV FINANCE_ROOT=/data/target
ENV FTK_PYTHON_VERSION=3.10

ENTRYPOINT ["/app/finance-toolkit/entrypoint.sh"]
