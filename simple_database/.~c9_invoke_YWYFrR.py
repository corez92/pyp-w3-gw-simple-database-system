from operator import attrgetter
from exceptions import ValidationError

def create_database(db_name):
    if db_name in Database.all_dbs:
        raise ValidationError('Database with name "{}" already exists.'.format(db_name))
    return Database(db_name)

def connect_database(db_name):
    pass

class Database(object):
    
    all_dbs = []
    
    def __init__(self, db_name):
        self.db_name = db_name
        self.all_tables = {}
            self.all = columns
    def create_table(self, table_name, columns=None):
        if not columns:
            raise Exception
        else:
            # create a new attribute
            setattr(self, table_name, Table(table_name, columns))
            self.all_tables[table_name] = Table(table_name, columns)
    
    def show_tables(self):
        return [table for table in self.all_tables]


class Table(object):
    def __init__(self, name, fields):
        self.name = name
        self.fields = fields
        self.table = []


    def is_valid(self, entry):
        if len(entry) != len(self.fields):
            return "Invalid amount of field"
        for index, cell in enumerate(entry):
            cell_type = type(cell).__name__
            if self.fields[index]['type'] != cell_type :
                return 'Invalid type of field "{}": Given "{}", expected "{}"'.format(
                        self.fields[index]['name'], cell_type, self.fields[index]['type'])
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
            {field['name'] : cell for field, cell in zip(self.fields, args)}
            ))
        
    def sort_by(self,attribute=None): 
        if attribute:
            self.table.sort(key = attrgetter(attribute), reverse = True)
        else:
            self.table.sort(key = attrgetter('id'), reverse = True)

    def describe(self):
        return self.fields
        
    def query(self, **kwargs):
        key, value = kwargs.items()[0]
        return [entry for entry in self.table if getattr(entry,key) == value]
    
class Row(object):
    def __init__(self,a_row):
         
        for key in a_row:
            setattr(self,key,a_row[key])