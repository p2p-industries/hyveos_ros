FROM ros:jazzy

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y python3-pip

COPY devcontainer.env .
RUN $(cat devcontainer.env) && \
    git clone "https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/p2p-industries/hyveos_ros_msgs.git" /home/p2p-industries/src/hyveos_msgs
RUN $(cat devcontainer.env) && \
    python3 -m pip install -U --break-system-packages "git+https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/p2p-industries/hyveos.git#subdirectory=sdks/python"
RUN rm devcontainer.env

ENV SHELL=/bin/bash

WORKDIR /home/p2p-industries/src/hyveos

CMD ["/bin/bash"]
