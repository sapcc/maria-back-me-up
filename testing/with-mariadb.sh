#!/bin/bash

# SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company
#
# SPDX-License-Identifier: Apache-2.0

set -eo pipefail
# set -x

if hash greadlink &>/dev/null; then
  readlink() { greadlink "$@"; }
fi

# set working directory to repo root
cd "$(dirname "$(dirname "$(readlink -f "$0")")")"

echo 'Initializing database'
mariadb-install-db --user=mysql --datadir=/var/lib/mysql --basedir=/usr
#chown -R mysql: testing/mysql/
echo 'Database initialized'

mariadbd-safe --skip-networking=0 --nowatch --log-bin=mysqld-bin --binlog-row-metadata=FULL
mariadb_options=(--protocol=socket -uroot)

execute() {
    statement="$1"
    if [ -n "$statement" ]; then
        mariadb -ss "${mariadb_options[@]}" -e "$statement"
    else
        cat /dev/stdin | mariadb -ss "${mariadb_options[@]}"
    fi
}

for i in $(seq 30 -1 0); do
    if execute 'SELECT 1' &> /dev/null; then
        break
    fi
    echo 'MariaDB init process in progress...'
    sleep 1
done
if [ "$i" = 0 ]; then
    echo >&2 'MariaDB init process failed.'
    exit 1
fi

echo "Setting root password..."

sudo mariadb -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('test');"

echo "Creating test database..."

sudo mariadb -e "CREATE DATABASE IF NOT EXISTS test"

echo "Creating service database"

sudo mariadb -e "CREATE DATABASE IF NOT EXISTS service"

echo "Creating table tasks in service database"

sudo mariadb -e "CREATE TABLE IF NOT EXISTS service.tasks ( \
    task_id INT AUTO_INCREMENT PRIMARY KEY, \
    title VARCHAR(255) NOT NULL, \
    start_date DATE, \
    due_date DATE, \
    description TEXT \
    ) ENGINE=INNODB;" \

echo "Inserting data into tasks table..."


insert1="INSERT INTO service.tasks (title, start_date, due_date, description) \
        VALUES('task1', '2021-05-02', '2022-05-02', 'task info 1');"


insert2="INSERT INTO service.tasks (title, start_date, due_date, description) \
        VALUES('task2', '2021-05-02', '2022-05-02', 'task info 2');"


insert3="INSERT INTO service.tasks (title, start_date, due_date, description) \
        VALUES('task3', '2021-05-02', '2022-05-02', 'task info 3');"


insert4="INSERT INTO service.tasks (title, start_date, due_date, description) \
        VALUES('task4', '2021-05-02', '2022-05-02', 'task info 4');"

sudo mariadb -e "$insert1"
sudo mariadb -e "$insert2"
sudo mariadb -e "$insert3"
sudo mariadb -e "$insert4"


echo "Inserting dummy data into tasks table finished"

echo "Starting background check to restart MariaDB if shutdown"
x=0
while :
  do
    mariadb-admin -h localhost -uroot -ptest ping > /dev/null || x=1
    if [ $x -eq 0 ]
    then
      sleep 5
    else
      sleep 5
      echo "mariadb is down. restarting."
      mariadbd-safe --skip-networking=0 --nowatch --log-bin=mysqld-bin --binlog-row-metadata=FULL
      sleep 10
      x=0
    fi
done &

echo "Setting up secondary mariadb for streaming tests"
mariadb-install-db --user=mysql --datadir=/var/lib/mysqlstream --basedir=/usr
mariadbd-safe --skip-networking=0 --nowatch --socket=/tmp/mysqlstream.sock --port=3307 --datadir=/var/lib/mysqlstream

mariadb_options=(--protocol=socket -uroot -S/tmp/mysqlstream.sock)

execute() {
    statement="$1"
    if [ -n "$statement" ]; then
        mariadb -ss "${mariadb_options[@]}" -e "$statement"
    else
        cat /dev/stdin | mariadb -ss "${mariadb_options[@]}"
    fi
}

for i in $(seq 30 -1 0); do
    if execute 'SELECT 1' &> /dev/null; then
        break
    fi
    echo 'MariaDB init process in progress...'
    sleep 1
done
if [ "$i" = 0 ]; then
    echo >&2 'MariaDB init process failed.'
    exit 1
fi

sudo mariadb -S/tmp/mysqlstream.sock -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('streaming');"

echo "Running command: $*"
set +e
"$@"
EXIT_CODE=$?
set -e

exit "${EXIT_CODE}"
