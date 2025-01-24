# Base image
FROM quay.io/astronomer/astro-runtime:12.6.0

# Switch to root to install system dependencies
USER root

# Install Terraform, Azure CLI, and Git
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget unzip curl git && \
    # Install Terraform
    wget -q https://releases.hashicorp.com/terraform/1.9.8/terraform_1.9.8_linux_amd64.zip && \
    unzip terraform_1.9.8_linux_amd64.zip -d /usr/local/bin && \
    rm terraform_1.9.8_linux_amd64.zip && \
    # Install Azure CLI
    curl -sL https://aka.ms/InstallAzureCLIDeb | bash && \
    # Cleanup
    apt-get remove --purge -y wget unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the default 'astro' user    
USER astro
