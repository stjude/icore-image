FROM eclipse-temurin:8
WORKDIR /root
COPY ./ctp /root/ctp
COPY ./aiminer.py /root/aiminer.py
RUN ln -s /config.yml /root/config.yml
RUN ln -s /input /root/input
RUN ln -s /output /root/output
RUN apt-get update --fix-missing && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-requests \
        python3-pandas \
        python3-yaml \
        python3-lark \
        dcmtk
CMD python3 aiminer.py