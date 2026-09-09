FROM python:3.12
WORKDIR /usr/src/app
RUN pip install uv
COPY ./backend/uv.lock uv.lock
COPY ./backend/pyproject.toml pyproject.toml
RUN uv sync
COPY ./backend .