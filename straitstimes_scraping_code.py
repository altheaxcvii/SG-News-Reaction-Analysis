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
        # Create the progress bar
        progress_bar = tqdm(total=total_messages, desc='Fetching Messages', unit='message')

        async for message in client.iter_messages('TheStraitsTimes', limit=total_messages):
            #get reactions
            reaction_data = []
            if message.reactions and isinstance(message.reactions, MessageReactions):
                for reaction in message.reactions.results:
                    emoticon = reaction.reaction.emoticon
                    count = reaction.count
                    reaction_data.append({emoticon: count})

            #message details
            row = {
                'message_id': message.id,
                'date': message.date,
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
#removing messages with no text
df = df[df['text'] != '']

#I will filter out the earlier messages before Straits Times allowed reactions
df = df[df['date'] > '2022-07']

df['url'] = df['text'].str.extract('(http.*)')[0]
df = df[df['url'].notna()]
df['main_text'] = df['text'].str.split('\n')
df = df[(df['main_text'].apply(len) == 1) | (df['main_text'].apply(len) == 3) | (df['main_text'].apply(len) == 2) | (df['main_text'].apply(len) == 4) | (df['main_text'].apply(len) == 5)]
df['joined_text'] = df['main_text'].apply(lambda x: ' '.join(filter(None, x)))
df['joined_text'] = df['joined_text'].str.strip()

# %%
def remove_urls(text):
    return re.sub(r'http\S+', '', text).strip()
df['joined_text'] = df['joined_text'].apply(remove_urls)

# %%
# Convert from UTC to SGT (which is 'Asia/Singapore' in pytz)
df['date'] = df['date'].dt.tz_convert('Asia/Singapore')

# Convert datetime to string
df['time'] = df['date'].dt.strftime('%H:%M')
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

# %%
def extract_shortened_url(url):
    pattern = r'\bhttps?://str\.sg/\w+\b'
    match = re.search(pattern, url)
    if match:
        return match.group()
    else:
        return url
    
df['url'] = df['url'].apply(extract_shortened_url)
df = df[~df['url'].str.len().isin([23, 27])]

# %%
import warnings
import urllib3
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# %%
def get_web_text(link):
    try:
        response = requests.get(link, headers ={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'}, verify=False)
        response.raise_for_status() 

        soup = BeautifulSoup(response.text, 'lxml')
        para = soup.find_all("div", class_="clearfix text-formatted field field--name-field-paragraph-text field--type-text-long field--label-hidden field__item")
        p_contents = re.findall(r'<p>(.*?)</p>', str(para), re.DOTALL)
        for i in range(len(p_contents)):
            p_contents[i] = re.sub(r'<[^>]+>', '', p_contents[i])
        filtered_list = list(filter(None, p_contents))
        filtered_list_no_whitespace = [s.strip() for s in filtered_list]
        result_string = ' '.join(filtered_list_no_whitespace)
        return result_string
    except (requests.RequestException, ValueError) as e:
        print(f"Error occurred for URL: {link}")
        print(e)
        return None

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

    progress_bar = tqdm(total=len(links_list), desc='Processing URLs')

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
df = df[~df['web_text'].isnull()]
df = df[df['web_text'] != '']

df.to_excel('Dataset/thestraitstimes.xlsx', index=False)
print('Scraping Completed 😎')