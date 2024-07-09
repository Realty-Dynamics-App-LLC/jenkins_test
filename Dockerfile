FROM continuumio/miniconda3

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create and set the working directory
WORKDIR /app

# Copy the environment.yml file to the container
COPY jenkins.yml /app/

# Create the Conda environment
RUN conda env create -f jenkins.yml

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "jenkins", "/bin/bash", "-c"]

# Copy the rest of the application code to the container
COPY . /app/

# Install pip dependencies within the Conda environment
RUN conda run -n jenkins pip install sqlalchemy==1.4.52 geoalchemy2==0.9.2 psycopg2-binary

# Make sure the script runs with the conda environment
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "jenkins", "python", "loadcsv_db.py"]
