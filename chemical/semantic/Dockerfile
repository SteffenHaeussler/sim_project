FROM rayproject/ray:latest-py312-cpu

RUN pip install torch transformers
RUN sudo apt-get update && sudo apt-get install curl -y

COPY . /app

WORKDIR /app

# # Copies the local `fake.py` file into the WORKDIR
# COPY config.yaml /serve_app/config.yaml
# COPY bi_encoder.py /serve_app/bi_encoder.py
# COPY cross_encoder.py /serve_app/cross_encoder.py

EXPOSE 8000
EXPOSE 8265

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "bash", "/entrypoint.sh" ]
