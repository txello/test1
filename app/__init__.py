import mongoengine as mng
from mongoengine.document import Document
from mongoengine import ObjectIdField, IntField, DateField
from bson import ObjectId
from datetime import datetime
import pandas as pd

mng.connect('bot',alias='dbbot')

class bot(Document):
    meta = {"db_alias": "dbbot"}
    _id = ObjectIdField(default=ObjectId)
    value = IntField()
    dt = DateField()

def mongoparse(start_date:datetime,end_date:datetime,group_type:str):
    start_date = datetime(2022, 9, 1)
    end_date = datetime(2022, 12, 31, 23 ,59)

    arr = pd.DataFrame([user.value for user in bot.objects(dt__gte=start_date, dt__lte=end_date)])
    return arr
    
arr = mongoparse(1,1,1)