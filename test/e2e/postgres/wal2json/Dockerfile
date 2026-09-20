# The official postgres image does not ship wal2json. Install the plugin from
# the PGDG repository (already configured in the image) so the runner can
# create a slot with plugin=wal2json (#153).
FROM postgres:17

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-17-wal2json \
    && rm -rf /var/lib/apt/lists/*
