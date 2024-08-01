import os

SQLITE_DB = os.getenv('SQLITE_DB', './data-20240703190001.db')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'strapi')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'strapi')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'strapi')
