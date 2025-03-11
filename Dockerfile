FROM eclipse-temurin:8
WORKDIR /root
COPY ./ctp /root/ctp
COPY ./aiminer.py /root/aiminer.py
COPY ./requirements_docker.txt /root/requirements_docker.txt
COPY ./scrubber.19.0403.lnx /root/scrubber.19.0403.lnx
RUN ln -s /config.yml /root/config.yml &&\
    ln -s /input /root/input &&\
    ln -s /output /root/output
RUN apt-get update --fix-missing &&\
    apt-get install -y dcmtk python3 python3-pip python3-venv &&\
    apt-get clean
RUN python3 -m venv venv && \
    . venv/bin/activate && \
    pip install -r requirements_docker.txt
CMD ["/root/venv/bin/python", "aiminer.py"]
