
from hanadb import Base
from sqlalchemy import Column, Integer, select
from typing import Optional
from sqlalchemy.exc import InvalidRequestError, ArgumentError

class SeregasInvalidRequestError(InvalidRequestError):
    """
    Now it's my exception! >:D
    """
    

class ModelFabric:
    _has_PKey = False
    _has_table_name = False
    _reserved_names = set(( 'get_columns', ))
    def __init__(self, tbname:Optional[str]=None):
        self.init_base_model()
        
        if tbname:
            self.set_table_name(tbname)

    def init_base_model(self):
        """
            Initialazes abstract sqlalchemy DeclarativeMeta Base class
        """
        class AbstractBase(Base):
            __abstract__ =True
        
        self.AbstractBase = AbstractBase

    def set_table_name(self, tbname:str):   
        self._has_table_name = True 
        setattr(self.AbstractBase, '__tablename__', tbname)  
        
    def add_column(self, cname:str, column:Column):
        if cname in self._reserved_names:
            raise SeregasInvalidRequestError(
                f"Attribute name {cname} is reserved when using the Declarative API in Sereja's realisation")

        if column.primary_key:
            self._has_PKey = True
        setattr(self.AbstractBase, cname, column)
      
    def compile(self):
        if not self._has_table_name:
            raise InvalidRequestError('Table name not specified')
        if not self._has_PKey:
            raise ArgumentError('Could not assemble any primary key columns for mapped table')
        
        class FabricBase(self.AbstractBase):
            @staticmethod
            def get_columns():
                ret_lst = [attr for attr in dir(FabricBase) if attr[0] != '_']
                ret_lst.pop(ret_lst.index('get_columns'))
                ret_lst.pop(ret_lst.index('metadata'))
                ret_lst.pop(ret_lst.index('registry'))
                return ret_lst

        return FabricBase
        

if __name__ == "__main__":
    myModelFabric = ModelFabric()
    myModelFabric.set_table_name('maRa')
    myModelFabric.add_column('MARA',  Column(Integer, primary_key=False))
    try:
        myModelFabric.add_column('_has_PKey',  Column(Integer, primary_key=False))
    except InvalidRequestError:
        print("Ну, да. Не работает")
    try:
        myModelFabric.compile()
    except ArgumentError:
        print('Ну а что ты хотел')
    myModelFabric.add_column('HANA',  Column(Integer, primary_key=True))
    
    MyModel1 = myModelFabric.compile()

    print(select(MyModel1))


    myModelFabric.init_base_model()
    
    try:
        MyModel2 = myModelFabric.compile()
    except InvalidRequestError:
        print('Признайся, ты этого и ждал')
    except AttributeError:
        print('Признайся, ты этого и ждал')
 
    myModelFabric.set_table_name('rama')
    myModelFabric.add_column('PollY',  Column(Integer, primary_key=True))

    MyModel2 = myModelFabric.compile()

    print(select(MyModel1))
    print(select(MyModel2))

    print(MyModel1.get_columns())