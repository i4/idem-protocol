FROM ubuntu:jammy
ARG LOCALUID
ARG LOCALGID
# Install depencendies
RUN apt update && apt -y install git vim openjdk-18-jdk make rsync tmux ssh moreutils gnuplot librsvg2-bin
# Setup idem user and environment
RUN groupadd -g $LOCALGID idem && useradd -m -u $LOCALUID -g idem -s /bin/bash idem
USER idem
RUN mkdir /home/idem/.ssh && \
    ssh-keygen -t rsa -q -f "/home/idem/.ssh/id_rsa" -N "" && \
    cat /home/idem/.ssh/id_rsa.pub >> /home/idem/.ssh/authorized_keys && \
    mkdir -p /home/idem/idem-framework
VOLUME /home/idem/idem-framework
WORKDIR /home/idem/idem-framework

# Setup localhost SSH and start queue runner (we require root for this briefly)
USER root
ENTRYPOINT service ssh start && \
           su idem -c "ssh-keyscan localhost >> /home/idem/.ssh/known_hosts" && \
           su idem -c "tmux new-session -d -c /home/idem idem-framework/scripts/queue/queue_runner.py" && \
           su idem

