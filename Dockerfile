FROM python:3.11.4-slim-bullseye
WORKDIR /root
COPY ./ctp /root/ctp
COPY ./aiminer.py /root/aiminer.py
COPY ./scrubber.19.0403.lnx /root/scrubber.19.0403.lnx
RUN ln -s /config.yml /root/config.yml
RUN ln -s /input /root/input
RUN ln -s /output /root/output
RUN apt-get update --fix-missing && \
    apt-get install -y dcmtk python3 python3-pip && \
    apt-get clean
RUN pip install bcrypt lark openpyxl pandas pypdf2 python-docx pyyaml requests ruamel.yaml
CMD python3 aiminer.py
