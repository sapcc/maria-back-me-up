#!/bin/sh

set -eo pipefail
# set -x

if hash greadlink &>/dev/null; then
  readlink() { greadlink "$@"; }
fi

# set working directory to repo root
cd "$(dirname "$(dirname "$(readlink -f "$0")")")"

mkdir -p testing/mysql
chown mysql: testing/mysql

echo 'Initializing database'
mysql_install_db --user=mysql --datadir=testing/mysql --rpm
chown -R mysql: "$DATA_DIR"
echo 'Database initialized'

mysqld_safe --skip-networking --nowatch
mysql_options='--protocol=socket -uroot --log-bin=mysqld-bin --binlog-format=MIXED'

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

root_password=mypass
 
# Make sure that NOBODY can access the server without a password
sudo mysql -e "UPDATE mysql.user SET Password = PASSWORD('$root_password') WHERE User = 'root'"

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
