# Builds the docker image to run tests against.
FROM ubuntu:jammy
LABEL maintainer="FUC@nobody.com"

# Base requirements & secrethub
RUN apt-get update
RUN apt-get install -y apt-utils curl gnupg2
RUN curl -fsSL https://apt.secrethub.io/pub | apt-key add -
RUN echo "deb [trusted=yes] https://apt.secrethub.io stable main" > /etc/apt/sources.list.d/secrethub.sources.list
RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates git build-essential python3-dev python3-pip libyajl2 jq  mariadb-client mariadb-server secrethub-cli

# Get deps
RUN git clone https://gitlab.com/FUC/cogBot/ cogTest
RUN python3 /cogTest/setup.py deps --force=yes

# Prepare db
RUN curl "http://starcraftman.com/elite/eddb_v03.sql" > "eddb.sql"
RUN /etc/init.d/mariadb start && mysql -u root --password=root < "cogTest/tests/mysql_tables.sql" && mysql -u root --password=root -D eddb < "eddb.sql"

# Remove project
RUN rm -rf /cogTest

ENTRYPOINT ["/bin/bash"]
