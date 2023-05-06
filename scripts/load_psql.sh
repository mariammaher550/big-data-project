#!/bin/bash
psql -U postgres -c 'DROP DATABASE IF EXISTS project;'
psql -U postgres -c'CREATE DATABASE project;'

psql -U postgres -d project -f db.sql

psql -U postgres -c "select * from users;"