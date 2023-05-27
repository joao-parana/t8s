FROM python:3.11.3-slim-bullseye

# For details on this image, see: https://realpython.com/docker-continuous-integration/ 

# Install dependencies
RUN apt-get update && apt-get upgrade --yes

# Create a non-root user and switch to it
RUN useradd --create-home gamma
USER gamma
WORKDIR /home/gamma

# Set up a virtual environment
ENV VIRTUALENV=/home/gamma/.venv
RUN python3 -m venv $VIRTUALENV
ENV PATH="$VIRTUALENV/bin:$PATH"

# Copy requirements
COPY --chown=gamma pyproject.toml constraints.txt ./
COPY --chown=gamma src/ src/
COPY --chown=gamma tests/ tests/
COPY --chown=gamma main.py ./
COPY --chown=gamma smoke.py ./
COPY --chown=gamma README.md ./

# Install requirements
RUN python -m pip install --upgrade pip setuptools hatch && \
    python -m pip install --no-cache-dir -c constraints.txt ".[dev]"

# Install dependencies and run tests
RUN python -m pip install . -c constraints.txt && \
    python smoke.py && \
    python -m pytest tests/

# For now I will ignore some rules of flake8, isort, black, pylint and bandit    
# RUN python -m flake8  --ignore=E501,E302,E305,W291 src/ && \
    # python -m isort src/ --check && \
    # python -m black src/  && \
    # python -m pylint src/ --disable=C0114,C0116,R1705 && \
    # python -m bandit -r src/ --quiet

CMD ["python3", "-c",  "'import t8s ; print(t8s.__version__)'"]
