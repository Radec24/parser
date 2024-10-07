import logging
from telethon import TelegramClient, events, utils
from telethon.errors import FloodWaitError, ServerError
import re
import os
from telethon.tl.types import Channel, Chat, User
import asyncio
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to DEBUG for more verbosity
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),  # Log to bot.log file
        logging.StreamHandler()  # Also output to console
    ]
)

# Retrieve credentials from environment variables
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')  # Bot token to send messages

if not API_ID or not API_HASH or not BOT_TOKEN:
    logging.error("API_ID, API_HASH, and BOT_TOKEN must be set as environment variables.")
    exit(1)

# Initialize the Telegram client for user account (listening)
client = TelegramClient('account_session', API_ID, API_HASH)

# Initialize the Telegram client for bot (sending messages)
bot_client = TelegramClient('bot', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

logging.info("Telegram client started as a user account for listening.")
logging.info("Bot client started for sending messages.")

# Define keyword groups
keyword_groups = [
    {
        'name': 'RentaCar',
        'keywords_file': 'rentacar_keywords.txt',  # Name of the keywords file
        'excluded_words_file': 'rentacar_excluded_words.txt',  # Name of the excluded words file
        'target_chat_id': int('-1002156056838'),  # Replace with your target_chat_id
        'csv_file': 'RentaCar.csv'  # Name of the CSV file to write data
    }
]

# Load keywords and excluded words into memory
keyword_data = {}

for group in keyword_groups:
    keywords = []
    with open(group['keywords_file'], 'r', encoding='utf-8') as file:
        keywords = [line.strip().lower() for line in file]
    excluded_words = []
    with open(group['excluded_words_file'], 'r', encoding='utf-8') as file:
        excluded_words = [line.strip().lower() for line in file]

    # Log loaded keywords and excluded words
    logging.info(f"Loaded keywords for {group['name']}: {keywords}")
    logging.info(f"Loaded excluded words for {group['name']}: {excluded_words}")

    keyword_data[group['name']] = {
        'keywords': keywords,
        'excluded_words': excluded_words
    }

# Retry parameters
MAX_RETRIES = 5
RETRY_DELAY = 10


@client.on(events.NewMessage)
async def handle_new_message(event):
    message = event.message
    
    # Logging to verify that the bot is receiving messages from all chats
    logging.info(f"Received message in chat {message.chat_id} from sender {message.sender_id}")
    
    # Log the message text for debugging
    if message.text:
        logging.info(f"Message text: {message.text}")
    else:
        logging.info("Message does not contain text. Ignoring...")
        return

    for group in keyword_groups:
        group_name = group['name']
        keywords = keyword_data[group_name]['keywords']
        excluded_words = keyword_data[group_name]['excluded_words']
        target_chat_id = group['target_chat_id']

        # Check for keywords and excluded words
        for keyword in keywords:
            pattern = fr'(?i)\b{re.escape(keyword)}\b'
            if re.search(pattern, message.text) and not any(
                    excluded_word in message.text.lower() for excluded_word in excluded_words):
                logging.info(f"Keyword '{keyword}' found in message from {message.sender_id}")

                user_id = message.sender_id
                username = None
                try:
                    user = await client.get_entity(user_id)
                    if isinstance(user, User):
                        username = user.username
                        display_name = utils.get_display_name(user)
                        user_link = f'<a href="tg://user?id={user_id}">{display_name}</a>'
                        notification = f'Найдено ключевое слово "{keyword}" в сообщении от пользователя {user_link} (@{username}):\n\n{message.text}'
                    else:
                        raise ValueError()
                except (ValueError, TypeError):
                    chat_id = message.chat_id
                    chat = await client.get_entity(chat_id)
                    if isinstance(chat, Channel) or isinstance(chat, Chat):
                        if chat.username:
                            chat_username = chat.username
                            chat_link = f'<a href="https://t.me/{chat_username}/{message.id}">ссылка на сообщение</a>'
                            notification = f'Найдено ключевое слово "{keyword}" в сообщении от пользователя с недоступным именем. Ссылка на сообщение: {chat_link}:\n\n{message.text}'

                # Send the message using the bot
                try:
                    await bot_client.send_message(entity=target_chat_id, message=notification, parse_mode='html')
                    logging.info(f"Sent notification to chat {target_chat_id} via bot")
                except FloodWaitError as e:
                    logging.warning(f'FloodWaitError: Pausing for {e.seconds} seconds due to rate limiting.')
                    await asyncio.sleep(e.seconds)
                break

# Start the user client to listen for messages
logging.info("Starting Telegram client for listening...")
with client:
    client.run_until_disconnected()

# Start the bot client for sending messages
with bot_client:
    bot_client.run_until_disconnected()
