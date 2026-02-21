from bs4 import BeautifulSoup
import pandas as pd
import requests as r

URL="https://docs.google.com/document/d/e/2PACX-1vTER-wL5E8YC9pxDx43gk8eIds59GtUUk4nJo_ZWagbnrH0NFvMXIw6VWFLpf5tWTZIT9P9oLIoFJ6A/pub"

response = r.get(URL)

df_list = pd.read_html(URL)
df_list[0].head()
