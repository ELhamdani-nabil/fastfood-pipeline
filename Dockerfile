FROM apache/spark:3.5.1-python3
USER root
RUN pip install --no-cache-dir jupyterlab pyspark==3.5.1 delta-spark==3.2.0 graphframes==0.6 kafka-python
ENV PYSPARK_PYTHON=python3
USER spark
