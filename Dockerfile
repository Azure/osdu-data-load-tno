# docker build --build-arg AZURE_TENANT=$AZURE_TENANT --file Dockerfile --tag osdu-data-load . 
# docker run -it -v $(pwd)/open-test-data:/app/open-test-data -v $(pwd)/output:/app/output --env-file .env --env DATA_PARTITION=$NAME-$PARTITION  --env CLIENT_ID=$CLIENT_ID --env CLIENT_SECRET=$CLIENT_SECRET osdu-data-load

FROM mcr.microsoft.com/mirror/docker/library/ubuntu:20.04 as base
ARG AZURE_TENANT
ARG USERNAME=app
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ARG NAME=platform
ARG PARTITION=opendes

# Install packages
RUN apt-get update \
  && apt-get -y --no-install-recommends install \
  python3 \
  python3-pip \
  jq \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Set Working Directory
WORKDIR /app

# Create Default User
RUN groupadd --gid $USER_GID $USERNAME \
  && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
  #
  # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
  && apt-get update \
  && apt-get install -y sudo \
  && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
  && chmod 0440 /etc/sudoers.d/$USERNAME \
  && chown -R $USERNAME:$USER_GID /app
USER $USERNAME


# Install Python Requirements
COPY requirements.txt requirements.txt
RUN echo "**** Install Python Requirements ****" && \
  pip install --upgrade pip && \
  pip install -r requirements.txt
RUN mkdir open-test-data && mkdir output

# Copy Load Scripts
COPY config config
COPY src src
COPY .env .env
COPY setup.py setup.py
COPY load.sh load.sh

# Generate Manifests
ENV AZURE_TENANT=$AZURE_TENANT
ENV DATA_PARTITION=$NAME-$PARTITION
ENV OSDU_ENDPOINT=https://$END_POINT
ENV LOGIN_ENDPOINT=https://login.microsoftonline.com/${AZURE_TENANT}/oauth2/v2.0/token
ENV ACL_OWNER=data.default.owners
ENV ACL_VIEWER=data.default.viewers
ENV DOMAIN=contoso.com
ENV LEGAL_TAG=${DATA_PARTITION}-open-test-data
ENV PIP_INSTALL=false

CMD /bin/bash
