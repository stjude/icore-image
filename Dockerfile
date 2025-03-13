FROM eclipse-temurin:8
WORKDIR /root
COPY ./ctp /root/ctp
COPY ./aiminer.py /root/aiminer.py
COPY ./scrubber.19.0403.lnx /root/scrubber.19.0403.lnx
RUN ln -s /config.yml /root/config.yml
RUN ln -s /input /root/input
RUN ln -s /output /root/output
RUN apt-get update --fix-missing &&\
    apt-get install -y curl dcmtk python3 python3-pip python3-venv unzip &&\
    apt-get clean
    curl https://rclone.org/install.sh | bash
RUN python3 -m venv venv && \
    . venv/bin/activate && \
    pip install bcrypt lark openpyxl pandas pypdf2 python-docx pyyaml requests ruamel.yaml
CMD ["/root/venv/bin/python", "aiminer.py"]
