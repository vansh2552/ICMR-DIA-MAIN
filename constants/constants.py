
from enum import Enum

class db_credentials(Enum):
    HOST = '127.0.0.1'
    DB = 'amr2021'
    USER = 'root'
    PASSWORD = 'soumya123'  


class webpage(Enum):
    LOGIN_PAGE = 'login.html'
    INDEX_PAGE = 'index.html'
    IMPORT_PAGE = 'import.html'
    INSTRUCTIONS_PAGE = 'instructions.html'; 
