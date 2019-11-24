FROM registry.access.redhat.com/ubi8/ubi:latest

COPY . .
RUN yum -y install python3-pip git pyyaml pyyaml-devel
RUN git clone https://github.com/RedHatInsights/insights-core.git && pip3 install -e insights-core
RUN pip3 install -e .
