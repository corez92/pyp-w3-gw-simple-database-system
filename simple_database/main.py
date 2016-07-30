import pickle
import os
from config import BASE_DB_FILE_PATH

from operator import attrgetter
from exceptions import ValidationError

def create_database(db_name):
    
    dbpath = os.path.join(BASE_DB_FILE_PATH, db_name)

    if os.path.exists(dbpath):
        raise ValidationError('Database with name "{}" already exists.'
                                .format(db_name))
    
    os.makedirs(dbpath)

    return Database(dbpath, db_name)

def connect_database(db_name):
    dbpath = os.path.join(BASE_DB_FILE_PATH, db_name)
    return Database(dbpath, db_name, [open(file,"rb") for file in os.listdir(dbpath)])
    

class Database(object):

    def __init__(self, db_path, db_name, tables=[]):
        self.path = db_path
        self.name = db_name
        self.tables = tables

    def create_table(self, table_name, columns=None):
        if not columns:
            raise Exception

        table = Table(self.path, table_name, columns)
        setattr(self, table_name, table)

        self.tables.append(table)
    
    def show_tables(self):
        return [table.name for table in self.tables]


class Table(object):
    def __init__(self, path, name, fields):
        self.path = os.path.join(path,name) + ".p"
        self.name = name
        self.fields = fields
        self.table = []
        self.save()

    def is_valid(self, entry):
        if len(entry) != len(self.fields):
            return "Invalid amount of field"
        for index, cell in enumerate(entry):
            cell_type = type(cell).__name__
            exp_type = self.fields[index]['type']
            field = self.fields[index]['name']
            if exp_type != cell_type:
                return 'Invalid type of field "{}": Given "{}", expected "{}"'.format(field, cell_type, exp_type)
        return ""
                
    def count(self):
        return len(self.table)
    
    def all(self):
        for entry in self.table:
            yield entry
                
    def insert(self, *args):
        if self.is_valid(args):
            raise ValidationError(self.is_valid(args))
        
        self.table.append(Row(
            { field['name']: cell for field, cell in zip(self.fields, args) }
            ))
        
        self.save()
        
    def sort_by(self,attribute=None): 
        if attribute:
            self.table.sort(key = attrgetter(attribute), reverse = True)
        else:
            self.table.sort(key = attrgetter('id'), reverse = True)
        self.save()

    def describe(self):
        return self.fields
        
    def query(self, **kwargs):
        key, value = kwargs.items()[0]
        return [entry for entry in self.table if getattr(entry,key) == value]

    def save(self):
        with open(self.path,'wb') as fileObject:
            pickle.dump(self, fileObject)

class Row(object):
    def __init__(self,a_row):
        for key in a_row:
            setattr(self,key,a_row[key])
            
        