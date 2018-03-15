import requests
from BeautifulSoup import BeautifulSoup
import pandas as pd
import re
import csv
from bs4 import BeautifulSoup
import spacy

nlp = spacy.load('en')

headers = {'x-api-key': 'secret'}

entity_types=['PERSON', 'ORG', 'FACILITY', 'PRODUCT', 'EVENT', 'WORK_OF_ART']

def google_scraper(search, num):
    address="https://www.google.com/search?gl=us&hl=en&tbm=nws&q=%s&tbs=qdr:d&num=%d" % (search,num)
    r = requests.get(address)
    resp = r.content
    soup = BeautifulSoup(resp.decode('utf-8','ignore'))

    url=[]
    description=[]
    outlet=[]
    headline=[]

    for x in soup.findAll('h3', attrs={'class':'r'}):
        p = x.a['href'][7:]
        url.append(p.rpartition('&sa')[0])
    
    a=soup.findAll('span', attrs={'class':'f'})
    b=soup.findAll('div', attrs={'class':'st'})
    c=soup.findAll('h3', attrs={'class':'r'}) 


    for x in b:
        p = re.compile(r'<.*?>')
        description.append(p.sub('',str(x)).replace('.','').replace('&#39;',"'").replace('&amp;','&').replace('&nbsp;',' ').replace('&quot;','"').replace('&#8220;','"').replace('&#8211;','"').replace('&#8212','-'))
    
    for x in a:
        p = re.compile(r'<.*?>')
        outlet.append(p.sub('',str(x)).replace('.','').replace('&#39;',"'").replace('&amp;','&').replace('&nbsp;',' ').replace('&quot;','"').replace('&#8220;','"').replace('&#8211;','"').replace('&#8212','-'))
    
    for x in c:
        p = re.compile(r'<.*?>')
        a = p.sub('',str(x)).replace('.','').replace('&#39;',"'").replace('&amp;','&').replace('&nbsp;',' ').replace('&quot;','"').replace('&#8220;','"').replace('&#8211;','"').replace('&#8212','-')
        a = a.encode('utf8')
        headline.append(a)
    
    urly=[]
    for x in url:
        urly.append(str(x))

    linkers=pd.DataFrame(zip(outlet, headline, urly, description))
    linkers.columns=['outlet', 'headline', 'url', 'description']
    
    return linkers

def call_url(url):
    mercury='https://mercury.postlight.com/parser?url='
    total_url = '{0}{1}'.format(mercury, url)
    call = requests.get(total_url, headers=headers)
    
    return call

def call_to_text(call):
    text=BeautifulSoup(call.json()['content'], 'html.parser').text
    text=text.strip()
    
    return text, call.json()['url']

def extract_entities(text, url):
    word=[]
    type_word=[]

    doc = nlp(text)
    ents = list(doc.ents)

    for i in ents:
        if str(i) != '':
            for x in entity_types:
                if i.label_ == x:
                    word.append(str(i))
                    type_word.append(str(i.label_))

    both=pd.DataFrame(zip(word, type_word), columns=['word', 'word_type'])
    both['url']=url
    both.columns=['word', 'word_type', 'url']
                      
    return both

def group(df):
    df=df.groupby(df.columns.tolist()).size().reset_index().rename(columns={0:'count'})
    #df=df.sort_values(['count'], ascending=False).reset_index(drop=True)
    
    return df
    
def combine(google):    
    text_list = []
    url_list = []
    call_list=[]
    df=pd.DataFrame(columns=['word', 'word_type', 'url', 'count'])

    for url in google['url']:
        call=call_url(url)
        call_list.append(call)

    for call in call_list:
        text, url = call_to_text(call)
        text_list.append(text)
        url_list.append(url)

    for text in text_list:
        for url in url_list:
            df_ent=extract_entities(text, url)
            df_ent=df_ent.groupby(df_ent.columns.tolist()).size().reset_index().rename(columns={0:'count'})
            df=pd.concat([df,df_ent])

    return df
