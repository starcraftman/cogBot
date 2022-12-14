# Builds the docker image to run tests against.
FROM ubuntu:jammy
LABEL maintainer="FUC@nobody.com"

# Base requirements & doppler
RUN apt-get update
RUN sudo apt-get install -y apt-transport-https ca-certificates curl gnupg2
RUN curl -sLf --retry 3 --tlsv1.2 --proto "=https" 'https://packages.doppler.com/public/cli/gpg.DE2A7741A397C129.key' | apt-key add -
RUN echo "deb https://packages.doppler.com/public/cli/deb/debian any-version main" | tee /etc/apt/sources.list.d/doppler-cli.list
RUN apt-get update
RUN apt-get install -y build-essential python3-dev python3-pip libyajl2 jq doppler

# Get deps
RUN git clone https://gitlab.com/FUC/cogBot/ cogTest
RUN python3 /cogTest/setup.py deps --force=yes

# Prepare db
RUN curl "http://starcraftman.com/elite/eddb_v05.sql" > "eddb.sql"
RUN /etc/init.d/mariadb start && mysql -u root --password=root < "cogTest/tests/mysql_tables.sql" && mysql -u root --password=root -D eddb < "eddb.sql"

# Remove project
RUN rm -rf /cogTest

ENTRYPOINT ["/bin/bash"]
