# %%
from telethon.sync import TelegramClient
import asyncio
import pandas as pd
import nest_asyncio
from telethon.tl.types import MessageMediaPoll
from telethon.tl.types import MessageReactions
import re
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm 
nest_asyncio.apply()

# %%
api_id = input('Input API ID: ')
api_hash = input('Input API Hash: ')
total_messages = int(input('How many messages to extract? Integer only: '))

async def get_messages():
    message_data = []
    async with TelegramClient('anon', api_id, api_hash) as client:
        #create progress bar
        progress_bar = tqdm(total=total_messages, desc='Fetching Messages', unit='message')

        async for message in client.iter_messages('mothershipsg', limit=total_messages):
            #check if the message is a poll
            poll_question = poll_choices = None
            if isinstance(message.media, MessageMediaPoll):
                poll_question = message.media.poll.question
                poll_choices = [choice.text for choice in message.media.poll.answers]
            
            #get reactions
            reaction_data = []
            if message.reactions and isinstance(message.reactions, MessageReactions):
                for reaction in message.reactions.results:
                    emoticon = reaction.reaction.emoticon
                    count = reaction.count
                    reaction_data.append({emoticon: count})
            #get message details
            row = {
                'message_id': message.id,
                'date': message.date,
                'poll_question': poll_question,
                'text': message.text,
                'reactions': reaction_data,
            }

            message_data.append(row)
            progress_bar.update()

    progress_bar.close()
    df = pd.DataFrame(message_data)
    return df


new_loop = asyncio.new_event_loop()
asyncio.set_event_loop(new_loop)
df = asyncio.run(get_messages())

# %%
#Removing poll messages
df = df[df['poll_question'].isnull()]
df = df.drop('poll_question', axis =1)

#removing messages with no text
df = df[df['text'] != '']

#There are very litte reactions when Telegram first launched reactions so I will filter out the earlier messages
df = df[df['date'] > '2022-02']

df['main_text'] = df['text'].str.split('\n')
df['url'] = df['text'].str.extract('(http.*)')[0]
df = df[df['url'].notna()]
df = df[(df['main_text'].apply(len) == 5) | (df['main_text'].apply(len) == 7) | (df['main_text'].apply(len) == 9) | (df['main_text'].apply(len) == 8)]

# %%
def join_except_last_three(strings_list):
    if isinstance(strings_list, list) and len(strings_list) >= 3:
        return ' '.join(strings_list[:-3]).strip()
    elif isinstance(strings_list, list):
        return ' '.join(strings_list).strip()
    else:
        return ''

# Apply the custom function to create a new column
df['joined_text'] = df['main_text'].apply(join_except_last_three)
df.sort_values(by='joined_text', ascending=True)

# %%
# Convert from UTC to SGT (which is 'Asia/Singapore' in pytz)
df['date'] = df['date'].dt.tz_convert('Asia/Singapore')

# Convert datetime to string
df['time'] = df['date'].dt.strftime('%H:%M')
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

# %%
def extract_shortened_url(url):
    pattern = r'\bhttps?://bit\.ly/\w+\b'
    match = re.search(pattern, url)
    if match:
        return match.group()
    else:
        return url
    
df['url'] = df['url'].apply(extract_shortened_url)
df = df[~df['url'].str.len().isin([28, 26, 24, 17, 196, 138, 55, 32, 29, 30, 206, 124, 58, 45])]

# %%
def get_web_text(link):
    try:
        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'lxml')
        para = soup.find_all("div", class_="content-article-wrap")
        p_contents = re.findall(r'<p>(.*?)</p>', str(para), re.DOTALL)
        for i in range(len(p_contents)):
            p_contents[i] = re.sub(r'<[^>]+>', '', p_contents[i])
        filtered_list = list(filter(None, p_contents))
        filtered_list_no_whitespace = [s.strip() for s in filtered_list]
        result_string = ' '.join(filtered_list_no_whitespace)
        return result_string.replace('Follow us on Telegram for the latest updates: https://t.me/mothershipsg ', '')
    except (requests.RequestException, ValueError) as e:
        print(f"Error occurred for URL: {link}")
        print(e)
        return None


# %%
import warnings
import urllib3
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# %%
#set the number of threads you want to use
workers = int(input('How many threads to use? Integers only: '))

from concurrent.futures import ThreadPoolExecutor
def process_links_with_multithreading(links):
    with ThreadPoolExecutor(max_workers=workers) as executor:  
        results = [executor.submit(get_web_text, link) for link in links]
        return [future.result() for future in results]

# %%
if __name__ == "__main__":
    links_list = df['url'].unique().tolist()

    progress_bar = tqdm(total=len(links_list), desc='Extracting Web Articles...')

    def update_progress(future):
        progress_bar.update(1)

    #process links using multi-threading
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(get_web_text, link) for link in links_list]
        for future in futures:
            future.add_done_callback(update_progress)
        results = [future.result() for future in futures]

    url_to_web_content = {link: content for link, content in zip(links_list, results)}
    df['web_text'] = df['url'].map(url_to_web_content)
    progress_bar.close()

# %%
df['web_text'] = df['web_text'].str.strip()
df = df[df['web_text'] != '']

# %%
df.to_excel('Dataset/mothershipsg.xlsx', index=False)
print('Scraping Completed 😎')