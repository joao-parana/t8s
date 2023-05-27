FROM python:3.11.3-slim-bullseye

RUN apt-get update && apt-get upgrade --yes

RUN useradd --create-home gamma
USER gamma
WORKDIR /home/gamma

ENV VIRTUALENV=/home/gamma/.venv
RUN python3 -m venv $VIRTUALENV
ENV PATH="$VIRTUALENV/bin:$PATH"

COPY --chown=gamma pyproject.toml constraints.txt ./
RUN python -m pip install --upgrade pip setuptools && \
    python -m pip install --no-cache-dir -c constraints.txt ".[dev]"

COPY --chown=gamma src/ src/
COPY --chown=gamma test/ test/
COPY --chown=gamma main.py ./

RUN python -m pip install . -c constraints.txt && \
    python -m pytest test/unit/ && \
    python -m flake8 src/ && \
    python -m isort src/ --check && \
    python -m black src/ --check --quiet && \
    python -m pylint src/ --disable=C0114,C0116,R1705 && \
    python -m bandit -r src/ --quiet
