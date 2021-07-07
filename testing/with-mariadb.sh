#!/bin/sh

set -eo pipefail
# set -x

if hash greadlink &>/dev/null; then
  readlink() { greadlink "$@"; }
fi

# set working directory to repo root
cd "$(dirname "$(dirname "$(readlink -f "$0")")")"

echo 'Initializing database'
mysql_install_db --user=mysql --datadir=/var/lib/mysql --basedir=/usr
#chown -R mysql: testing/mysql/
echo 'Database initialized'

mysqld_safe --skip-networking=0 --nowatch --log-bin=mysqld-bin
mysql_options='--protocol=socket -uroot'

execute() {
    statement="$1"
    if [ -n "$statement" ]; then
        mysql -ss $mysql_options -e "$statement"
    else
        cat /dev/stdin | mysql -ss $mysql_options
    fi
}

for i in `seq 30 -1 0`; do
    if execute 'SELECT 1' &> /dev/null; then
        break
    fi
    echo 'MySQL init process in progress...'
    sleep 1
done
if [ "$i" = 0 ]; then
    echo >&2 'MySQL init process failed.'
    exit 1
fi

echo "Setting root password..."

sudo mysql -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('test');"

echo "Creating test database..."
 
sudo mysql -e "CREATE DATABASE IF NOT EXISTS test"
 
echo "Creating service database"
 
sudo mysql -e "CREATE DATABASE IF NOT EXISTS service"
 
echo "Creating table tasks in service database"
 
sudo mysql -e "CREATE TABLE IF NOT EXISTS service.tasks ( \
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

sudo mysql -e "$insert1"
sudo mysql -e "$insert2"
sudo mysql -e "$insert3"
sudo mysql -e "$insert4"
 
 
echo "Inserting dummy data into tasks table finished"

echo "Starting background check to restart MariaDB if shutdown"
x=0
while :
  do
    mysqladmin -h localhost -uroot -ptest ping > /dev/null || x=1
    if [ $x -eq 0 ]
    then
      sleep 5
    else
      echo "mariadb is down. restarting."
      mysqld_safe --skip-networking=0 --nowatch --log-bin=mysqld-bin
      sleep 10
      x=0
    fi
done &

echo "Setting up secondary mariadb for streaming tests"
mysql_install_db --user=mysql --datadir=/var/lib/mysqlstream --basedir=/usr
mysqld_safe --skip-networking=0 --nowatch --socket=/tmp/mysqlstream.sock --port=3307 --datadir=/var/lib/mysqlstream

execute() {
    statement="$1"
    if [ -n "$statement" ]; then
        mysql -ss $mysql_options -e "$statement"
    else
        cat /dev/stdin | mysql -ss $mysql_options
    fi
}

for i in `seq 30 -1 0`; do
    if execute 'SELECT 1' &> /dev/null; then
        break
    fi
    echo 'MySQL init process in progress...'
    sleep 1
done
if [ "$i" = 0 ]; then
    echo >&2 'MySQL init process failed.'
    exit 1
fi

sudo mysql -S/tmp/mysqlstream.sock -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('streaming');"

echo "Running command: $@"
set +e
"$@"
EXIT_CODE=$?
set -e

exit "${EXIT_CODE}"
