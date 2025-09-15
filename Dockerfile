FROM continuumio/miniconda3:4.8.2

RUN mkdir /src
COPY . /src
WORKDIR /src

RUN pip install -e .
RUN pip install --ignore-installed -r requirements.txt

ENTRYPOINT ["python", "encode_file_transfer"]
CMD ["sync"]
