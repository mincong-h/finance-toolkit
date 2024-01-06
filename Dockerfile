FROM python:3.9
WORKDIR /app/finance-toolkit/
COPY . /app/finance-toolkit/
RUN python setup.py install

VOLUME ["/data/source", "/data/target"]
ENV DOWNLOAD_DIR=/data/source
ENV FINANCE_ROOT=/data/target
ENV FTK_PYTHON_VERSION=3.9

ENTRYPOINT ["/app/finance-toolkit/entrypoint.sh"]
