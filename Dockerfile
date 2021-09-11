FROM python:3.7
WORKDIR /app/finance-toolkit/
COPY . /app/finance-toolkit/
RUN python setup.py install
ENTRYPOINT ["finance-toolkit", "--help"]
