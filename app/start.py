from aiogram import Router,F
from aiogram.filters import CommandStart
from aiogram.types import Message,CallbackQuery
from libs.words import wStart
router = Router()

@router.message(CommandStart())
async def start(message:Message):
    await message.answer(wStart.a1.format(i = message.from_user.full_name))