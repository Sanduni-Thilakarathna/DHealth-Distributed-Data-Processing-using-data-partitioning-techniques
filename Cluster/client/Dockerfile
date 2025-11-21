FROM bitnami/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y python3 python3-pip

RUN ln -s /usr/bin/python3 /usr/bin/python

# (OPTIONAL) You can install extra Python packages if needed here
# RUN pip install pandas

USER 1001
