# API Lib
from ast import Str
from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
from typing import Union
import json

#python data Lib
import pandas as pd
import time, datetime

# DataBase Lib
from sqlalchemy import create_engine
import sqlalchemy
import pymysql
pymysql.install_as_MySQLdb()

# get Connection and get Engine for Maria DB
def connection_db(host_name, user_id, passwd, db_name, tag):
    if tag == "connection":
        conn=pymysql.connect(host=host_name,port=3306,user=user_id,password=passwd,db=db_name)
        return conn
    
    if tag == "engine":
        engine = create_engine(f"mysql://{user_id}:{passwd}@{host_name}/{db_name}")
        return engine

# API Start
app = FastAPI()

# Hello World print()
@app.get("/")
async def root():
    return {"message": "Hello World"}

# User Data Search Function
@app.get("/data/user_search")
async def user_search(user_age: int):
    conn = connection_db(host_name="localhost", user_id="root",passwd="wkdrnssla12!", db_name="testdb", tag="connection")
    
    SQLCommend = f"select * from user_info where user_age >= {user_age}"
    user_list = pd.read_sql(SQLCommend, conn)
    user_json = user_list.to_json(orient="records")
    
    return user_json

# User Update Body Class

class Item(BaseModel):
    user_id: str
    user_age: Optional[int] = None
    user_address: Optional[str] = None


@app.post("/user/update")
async def user_data_update(user_updata_data: Item):
    data = user_updata_data.dict()
    
    user_name = data["user_id"]
    SQLCommend = f"select * from user_info where {user_name}"
    # user_age = data["user_age"]
    # user_address = data["user_address"]
    
    engine = connection_db(host_name='localhost',user_id="root",passwd='wkdrnssla12!',db_name="testdb", tag = "engine")
    md = sqlalchemy.MetaData(engine)
    table = sqlalchemy.Table('user_info', md, autoload=True)
    upd = table.update().where(table.c.user_id==user_name).values(update_time=datetime.datetime.now())
    engine.execute(upd)
    
    # res  = {"resutl" : "update_complete"}
    # res = json.dumps(res)
    return user_updata_data

