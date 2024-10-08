from telethon import TelegramClient, events, utils
from telethon.errors import FloodWaitError
import re
from telethon.tl.types import Channel, Chat, User
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# User account credentials for listening
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')

# Bot token for sending notifications
bot_token = os.getenv('BOT_TOKEN')

# Paths to session files
account_session_file = 'account_session.session'
user_session_file = 'user_session.session'
bot_session_file = 'bot.session'

# Initialize the user client to listen to messages
user_client = TelegramClient(user_session_file, api_id, api_hash)

# Initialize the bot client for sending messages
bot_client = TelegramClient(bot_session_file, api_id, api_hash).start(bot_token=bot_token)

# Define the keyword groups
keyword_groups = [
    {
        'name': 'RentaCar',
        'keywords_file': 'rentacar_keywords.txt',  # Имя файла с ключевыми словами
        'excluded_words_file': 'rentacar_excluded_words.txt',  # Имя файла с исключающими словами
        'target_chat_id': int('-1002156056838'),  # Замените на нужный вам target_chat_id
        'csv_file': 'RentaCar.csv'  # Имя файла CSV для записи данных
    }
]

# Загрузка ключевых слов и исключающих слов в память
keyword_data = {}

for group in keyword_groups:
    keywords = []
    with open(group['keywords_file'], 'r', encoding='utf-8') as file:
        keywords = [line.strip().lower() for line in file]
    excluded_words = []
    with open(group['excluded_words_file'], 'r', encoding='utf-8') as file:
        excluded_words = [line.strip().lower() for line in file]
    keyword_data[group['name']] = {
        'keywords': keywords,
        'excluded_words': excluded_words
    }

# Store processed messages to prevent duplicates (message ID + chat ID)
processed_messages = {}

# Time to keep messages in memory before clearing (in seconds)
MESSAGE_EXPIRY_TIME = 24 * 60 * 60  # 24 hours

def clean_old_messages():
    """Clean up processed messages that are older than MESSAGE_EXPIRY_TIME."""
    current_time = time.time()
    expired = [key for key, timestamp in processed_messages.items() if current_time - timestamp > MESSAGE_EXPIRY_TIME]
    for key in expired:
        del processed_messages[key]

def create_message_link(chat, message_id):
    """Creates a valid message link for private or public chats."""
    if hasattr(chat, 'username') and chat.username:
        # For public channels or groups with a username
        return f'https://t.me/{chat.username}/{message_id}'
    else:
        # For private groups or channels
        chat_id_adjusted = chat.id - 1000000000000
        return f'https://t.me/c/{chat_id_adjusted}/{message_id}'

@user_client.on(events.NewMessage)  # Only the user client listens for new messages
async def handle_new_message(event):
    message = event.message

    # Prevent message loops by ignoring messages from the target chat
    for group in keyword_groups:
        if message.chat_id == group['target_chat_id']:
            return

    # Create a unique message key (chat_id + message_id)
    message_key = (message.chat_id, message.id)

    # Check if this message has already been processed
    if message_key in processed_messages:
        print(f"Message {message.id} from chat {message.chat_id} has already been processed.")
        return

    # Mark the message as processed
    processed_messages[message_key] = time.time()

    # Clean old messages (this can be done periodically, e.g., every X messages)
    clean_old_messages()

    # Process messages and check for keywords
    for group in keyword_groups:
        group_name = group['name']
        keywords = keyword_data[group_name]['keywords']
        excluded_words = keyword_data[group_name]['excluded_words']
        target_chat_id = group['target_chat_id']

        for keyword in keywords:
            pattern = fr'(?i)\b{re.escape(keyword)}\b'
            if re.search(pattern, message.text) and not any(
                    excluded_word in message.text.lower() for excluded_word in excluded_words):
                
                notification = None

                # Check if the sender is a user or a group/channel
                if isinstance(message.sender, User):  # Sender is a user
                    user_id = message.sender_id
                    display_name = utils.get_display_name(message.sender)

                    # Generate a user link (whether or not they have a username)
                    user_link = f'<a href="tg://user?id={user_id}">{display_name}</a> с ID {user_id}'
                    
                    # If the user has a username, append it to the message
                    if message.sender.username:
                        user_link += f" (@{message.sender.username})"
                    
                    # Create the source message link
                    chat = await user_client.get_entity(message.chat_id)
                    message_link = create_message_link(chat, message.id)

                    # Notification for user-sent message
                    notification = (
                        f'Найдено ключевое слово "{keyword}" в сообщении от пользователя {user_link}.\n'
                        f'ссылка на сообщение: <a href="{message_link}">ссылка на сообщение</a>:\n\n'
                        f'{message.text}'
                    )

                else:  # Sender is a group or channel
                    try:
                        chat = await user_client.get_entity(message.chat_id)
                        if isinstance(chat, (Channel, Chat)):
                            chat_name = chat.title if hasattr(chat, 'title') else 'неизвестный чат'
                            message_link = create_message_link(chat, message.id)
                            notification = (
                                f'Найдено ключевое слово "{keyword}" в сообщении от {chat_name}.\n'
                                f'Ссылка на сообщение: <a href="{message_link}">ссылка на сообщение</a>\n\n'
                                f'{message.text}'
                            )
                    except Exception as e:
                        print(f"Error fetching chat details for chat_id {message.chat_id}: {e}")

                # Send notification to the target chat using the bot
                if notification:
                    try:
                        await bot_client.send_message(entity=target_chat_id, message=notification, parse_mode='html')
                    except FloodWaitError as e:
                        print(f'FloodWaitError: Pausing for {e.seconds} seconds due to rate limiting.')
                break

# Run the user client for listening and the bot for sending notifications
with user_client:
    user_client.run_until_disconnected()
