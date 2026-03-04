# Use the official code-server image as a base
# This image runs as a non-root user 'coder' (UID 1000) by default.
FROM codercom/code-server:latest

# Set the working directory to the default project folder
WORKDIR /home/coder/project

# Install python3 and jq for the entrypoint script
USER root
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    jq \
    sqlite3 \
    r-base \
    libzmq3-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install and register kernels
RUN pip3 install --break-system-packages ipykernel \
    && R -e "install.packages('IRkernel', repos='https://cloud.r-project.org/'); IRkernel::installspec(user=FALSE)"

USER coder

# Install extensions
RUN code-server --install-extension RooVeterinaryInc.roo-cline \
    && code-server --install-extension ms-python.python \
    && code-server --install-extension ms-toolsai.jupyter \
    && code-server --install-extension REditorSupport.r

# Copy the entrypoint script
COPY --chown=coder:coder entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

# Expose the default port for code-server
EXPOSE 8080

# Use the entrypoint script to configure Cline at runtime
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
