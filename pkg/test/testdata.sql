DROP DATABASE IF EXISTS service;

CREATE DATABASE IF NOT EXISTS service;

CREATE TABLE IF NOT EXISTS service.tasks( 
  ask_id INT AUTO_INCREMENT PRIMARY KEY, 
    title VARCHAR(255) NOT NULL, 
    start_date DATE, 
    due_date DATE, 
    description TEXT 
    ) ENGINE=INNODB;

INSERT INTO service.tasks (title, start_date, due_date, description)
        VALUES('task1', '2021-05-02', '2022-05-02', 'task info 1');

INSERT INTO service.tasks (title, start_date, due_date, description)
        VALUES('task2', '2021-05-02', '2022-05-02', 'task info 2');

INSERT INTO service.tasks (title, start_date, due_date, description)
        VALUES('task3', '2021-05-02', '2022-05-02', 'task info 3');

INSERT INTO service.tasks (title, start_date, due_date, description)
        VALUES('task4', '2021-05-02', '2022-05-02', 'task info 4');

DROP DATABASE IF EXISTS application;

CREATE DATABASE IF NOT EXISTS application;

CREATE TABLE IF NOT EXISTS application.ratings( 
  app_id INT AUTO_INCREMENT PRIMARY KEY, 
    title VARCHAR(255) NOT NULL,  
    release_date DATE, 
    description TEXT,
    rating INT
    ) ENGINE=INNODB;

INSERT INTO application.ratings (title, release_date, description, rating)
        VALUES('app1', '2021-05-02', 'app info 1', 4);

INSERT INTO application.ratings (title, release_date, description, rating)
        VALUES('app2', '2021-05-02', 'app info 2', 5);

INSERT INTO application.ratings (title, release_date, description, rating)
        VALUES('app3', '2021-05-02','app info 3', 3);

INSERT INTO application.ratings (title, release_date, description, rating)
        VALUES('app4', '2021-05-02', 'app info 4', 0);