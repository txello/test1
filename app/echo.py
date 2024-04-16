from aiogram import Router
from aiogram.types import Message

from datetime import datetime
import json
from plugins.mongodb import mongoparse

router = Router()

@router.message()
async def echo(message:Message):
    arr = json.loads(message.text)
    arr = mongoparse(datetime.fromisoformat(arr['dt_from']),datetime.fromisoformat(arr['dt_upto']),arr['group_type'])
    arr = arr.to_dict()
    values = list(arr['total_value'].values())
    dates = list(arr['dt'].values())
    new_data = {
    "dataset": values[::-1],
    "labels": dates[::-1]
    }
    
    return await message.answer(json.dumps(new_data))