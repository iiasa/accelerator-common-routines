# Use the official code-server image as a base
# This image runs as a non-root user 'coder' (UID 1000) by default.
FROM codercom/code-server:latest

# Set the working directory to the default project folder
WORKDIR /home/coder/project

# Install python3, jq, sqlite3, sudo, and language dependencies
USER root
RUN apt-get update && apt-get install -y \
    sudo \
    python3 \
    python3-pip \
    python3-venv \
    python3-ipykernel \
    jq \
    sqlite3 \
    r-base \
    libzmq3-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    jupyter-core \
    jupyter-client \
    && rm -rf /var/lib/apt/lists/*

# Install and register R kernel system-wide
RUN R -e "install.packages('IRkernel', repos='https://cloud.r-project.org/'); IRkernel::installspec(user=FALSE)"

# Install R languageserver system-wide so the R extension doesn't prompt for it
# (libuv1-dev is needed to compile its 'fs' dependency)
RUN apt-get update && apt-get install -y libuv1-dev && rm -rf /var/lib/apt/lists/* \
    && R -e "install.packages('languageserver', repos='https://cloud.r-project.org/', Ncpus=parallel::detectCores()); \
    if (!requireNamespace('languageserver', quietly=TRUE)) quit(status=1)"

# All build steps run as root with HOME pointed at the coder home, so that
# rootless builders never have to exec a process as UID 65534 ('nobody').
ENV HOME=/home/coder

# --- R: Configure user-level library for packages ---
# Ensures install.packages() doesn't fail on permission errors or prompt interactively
RUN mkdir -p /home/coder/R/library && \
    echo 'R_LIBS_USER="~/R/library"' > /home/coder/.Renviron && \
    echo 'options(repos = c(CRAN = "https://cloud.r-project.org/"))' > /home/coder/.Rprofile

# --- Disable Settings ---
RUN mkdir -p /home/coder/.local/share/code-server/User && \
    echo '{"chat.disableAIFeatures": true, "chat.editor.enabled": false}' > /home/coder/.local/share/code-server/User/settings.json

# Install extensions
RUN code-server --install-extension Anthropic.claude-code \
    && code-server --install-extension Google.google-antigravity \
    && code-server --install-extension ms-python.python \
    && code-server --install-extension ms-toolsai.jupyter \
    && code-server --install-extension REditorSupport.r

# Copy the entrypoint script
COPY --chown=65534:65534 ./vscode/entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

# --- Workspace Permissions ---
# Ensure the runtime user owns everything created above
RUN chown -R 65534:65534 /home/coder

# Switch to the runtime user last; no RUN steps may follow this line
USER 65534

# Expose the default port for code-server
EXPOSE 8080

# Use the entrypoint script to launch code-server
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
