# Use the official code-server image as a base
# This image runs as a non-root user 'coder' (UID 1000) by default.
FROM codercom/code-server:latest

# Set the working directory to the default project folder
WORKDIR /home/coder/project

# Install python3 and jq for the entrypoint script
USER root
RUN apt-get update && apt-get install -y python3 jq sqlite3 && rm -rf /var/lib/apt/lists/*
USER coder

# Install the Cline extension (formerly known as Claude Dev)
# The extension ID is saoudrizwan.claude-dev
RUN code-server --install-extension RooVeterinaryInc.roo-cline

# Copy the entrypoint script
COPY --chown=coder:coder entrypoint.sh /usr/bin/entrypoint.sh
RUN chmod +x /usr/bin/entrypoint.sh

# Expose the default port for code-server
EXPOSE 8080

# Use the entrypoint script to configure Cline at runtime
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
