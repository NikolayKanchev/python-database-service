# python-database

A Database Service
You can:
  - register
  - login
  - create and delete database
  - create and delete tables in a database,
  - create and delete columns in a table
  - insert a row(some data) into a table
  - delete a row from a table
  - select functions are also implemented


All SQL statements that are implemented:

LOGIN USERNAME=nik PASSWORD=1234
REGISTER USERNAME=nik PASSWORD=1234
CREATE DATABASE animals
CREATE TABLE animals.cats
CREATE TABLE animals.cats (id int, name str, can_swim bool)
DROP DATABASE animals
DROP TABLE animals.cats
RENAME DATABASE animals TO users
RENAME TABLE animals.cats TO animals.dogs
ALTER TABLE animals.cats ADD name str
ALTER TABLE animals.cats DROP COLUMN name
ALTER TABLE animals.cats ALTER COLUMN name int
INSERT INTO animals.cats VALUES (1, Tom, False)
DELETE * FROM animals.cats
DELETE FROM animals.cats WHERE id=5
DELETE FROM animals.cats WHERE id=5 AND name=Tom ………
DELETE FROM animals.cats WHERE id=5 OR can_walk=Thrue ………
SELECT * FROM animals.cats
SELECT * FROM animals.cats WHERE id=1
SELECT * FROM animals.cats WHERE id=1 AND name=Tom AND …..
SELECT * FROM animals.cats WHERE id=1 OR name=Tom OR …..
SELECT id, name FROM animals.cats
SELECT id, name FROM animals.cats WHERE name=Tom
SELECT id, name FROM animals.cats WHERE name=Tom AND name=Cezar OR …...
SELECT id, name FROM animals.cats WHERE name=Tom OR name=Cezar OR …….
