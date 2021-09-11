FROM python:3.7-alpine
WORKDIR /app/finance-toolkit/
COPY . /app/finance-toolkit/
RUN python setup.py install
RUN python -m pip install -r requirements-tests.txt
