import pandas as pd
import numpy as np
import joblib
import os
import progressbar
from bs4 import BeautifulSoup
import re
import requests
from pandas.api.types import CategoricalDtype
import calendar
import datetime
import datefinder
from datetime import timedelta 
import streamlit as st

st.write('hello,world!')


def main(fails):
    '''
    Scans sites for new articles and updates database
    '''
    path = '../../data/raw/'
    
    try:
        df = articles_database()
    except:
        df = joblib.load(path+'all_joined.joblib')
        df.to_csv('../../data/processed/articles_database.csv')
    
    existing_urls = set(df[df.time_stamp > (pd.datetime.now() - timedelta(days=60))].url.values)

    existing_urls = set(df.url.values)

    print('scanning WHO articles...')
    who_new = WHO_download_new_articles(existing_urls=existing_urls)
    print('scanning OBNT articles...')
    obnt, obnt_bad_urls = OBNT_download_new_articles(existing_urls=existing_urls)
    #obnt = OBNT_download_new_articles(existing_urls=existing_urls)
    
    who_new.to_csv(f'../../data/processed/articles_database.csv', mode='a', header=False)
    print(f'WHO articles appended: {who_new.shape[0]}')

    obnt.to_csv(f'../../data/processed/articles_database.csv', mode='a', header=False)
    print(f'OBNT articles appended: {obnt.shape[0]}')

    cidrap_standard, cidrap_scan, cidrap_bad_urls_standard, cidrap_bad_urls_scan = cidrap_download_new_articles(existing_urls=existing_urls, old_failed=fails)
    
    cidrap_standard.to_csv(f'../../data/processed/articles_database.csv', mode='a', header=False)
    print(f'CIDRAP standard articles appended: {cidrap_standard.shape[0]}')
    
    cidrap_scan.to_csv(f'../../data/processed/articles_database.csv', mode='a', header=False)
    print(f'CIDRAP scan articles appended: {cidrap_scan.shape[0]}')
    
    if len(obnt_bad_urls) > 1:
        obnt_bad_urls.to_csv(f'../../data/processed/failed_urls_database.csv', mode='a', header=False)
        print(f'CIDRAP failed standard urls: {obnt_bad_urls.shape[0]}')

    if len(cidrap_bad_urls_standard) > 1:
        cidrap_bad_urls_standard.to_csv(f'../../data/processed/failed_urls_database.csv', mode='a', header=False)
        print(f'CIDRAP failed standard urls: {cidrap_bad_urls_standard.shape[0]}')

    if len(cidrap_bad_urls_scan) > 1:
        cidrap_bad_urls_scan.to_csv(f'../../data/processed/failed_urls_database.csv', mode='a', header=False)
        print(f'CIDRAP failed standard urls: {cidrap_bad_urls_scan.shape[0]}')
         
    num_records_updated = who_new.shape[0] + obnt.shape[0] + cidrap_standard.shape[0] + cidrap_scan.shape[0]

    print(f'Total records updated: {num_records_updated}')
    print(f'Time: {str(pd.datetime.now())[:19]}')
    print(f'Destination: ../../data/processed/articles_database.csv')


def articles_database(data='articles_database.csv'):
    '''
    Loads article csv database and converts format to datetime and categorical
    Return DataFrame    
    '''
    df = pd.read_csv(data).drop('Unnamed: 0', axis=1)
    df.time_stamp = pd.to_datetime(df.time_stamp)
    cat_source = CategoricalDtype(categories=['cidrap_scan', 'obnt', 'cidrap_standard', 'who'], ordered=True)
    df.source = df.source.astype(cat_source)
    return df


def failed_urls(data='../../data/processed/failed_urls_database.csv'):
    df = pd.read_csv(data).drop('Unnamed: 0', axis=1)
    df.time_stamp = pd.to_datetime(df.time_stamp)
    cat_source = CategoricalDtype(categories=['cidrap_scan', 'obnt', 'cidrap_standard', 'who'], ordered=True)
    df.source = df.source.astype(cat_source)
    return df


def WHO_year_urls(start_year=1996):
    '''
    Returns dictionary of WHO url(string) for each year(int)
    
    Parameters
    ----------
    start_year: int
    end_year: int   
    
    Returns
    -------
    dictionary
    '''    
    # list of year urls
    year_directory = [f'https://www.who.int/csr/don/archive/year/{i}/en/' for i in range(start_year, pd.datetime.now().year+1)]
    
    year_urls = {}
    for i in year_directory:
        url = i
        year_urls[int(re.findall(r'\d+', i)[0])] = i
    return year_urls


def articles_a_tag(who_year_urls):
    '''
    Returns list of all <a href> article tags
    '''
    all_article_tags = []
    for i,j in who_year_urls.items():

        url = j

        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        article_html = soup.find("ul", {"class": "auto_archive"})
        all_article_tags.extend(article_html.findAll('a'))
    return all_article_tags


def make_articles_dict(all_articles_tags):
    '''
    Converts dates to dd-mon-YYYY format
    Returns dictionary
        key: dates
        values: list of article urls   
    '''
    
    articles_dict = {}
    
    # mapping of month_name to number
    month_to_number = {v: k for k,v in enumerate(calendar.month_name)}
    
    for article in all_articles_tags:
        
        #split date and convert to datetime
        article_date = (article.string.split())
        article_datetime = datetime.datetime(year=int(article_date[2]), 
                                     month=month_to_number[article_date[1]], 
                                     day=int(article_date[0]))
        article_date = article_datetime.strftime("%d-%b-%Y")
        
        # append to list of articles per date if possible
        try:
            articles_dict[article_date] += ['https://www.who.int'+ article['href']]
        except:
            articles_dict[article_date] = ['https://www.who.int'+ article['href']]  

    return articles_dict


def WHO(url):
    '''
    Returns dictionary of title, html and raw text for specified WHO url.  
    
    Parameters
    ----------
    article URL
    
    Returns
    -------
    dictionary    
    '''
        
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    data = {}
    primary_id = soup.find(id="primary")
    data['html'] = primary_id.prettify(formatter='html')
    data['title'] = soup.title.string
    
    
    text = []
    for i in primary_id.stripped_strings:
        text.append(i)
    raw = ' '.join(text)
    data['text'] = raw
    
    return data


def update_WHO(articles_dict, existing_urls=[]):
    '''
    Loops through all WHO articles dictionary and downloads data, adding to 'database'
    
    Parameters
    ----------
    dictionary of article urls
    
    Returns
    -------
    dictionary of final data
    '''
    
    # convert index position to alphabet
    alpha_map = {x:j for x,j in enumerate('abcdefghijklmnopqrstuvwxyz')}
    database = []

    bar = progressbar.ProgressBar()

    for article_date, urls in bar(articles_dict.items()):
        for k, url in enumerate(urls):
            if url not in existing_urls:
                date_id = (article_date + alpha_map[k])
                data = WHO(url)
                data['source'] = 'who'
                data['url'] = url
                data['time_stamp'] = date_id
                database.append([data['time_stamp'], data['title'], data['url'], data['html'], data['text'], data['source']])
      
    return database


def WHO_download_new_articles(start_year='now', existing_urls=[]):
    '''
    Downloads all WHO articles that are not already in df.
    
    Parameters
    ----------
    start_year - int
    existing_urls - list of urls to ignore
        
    Returns
    -------
    DataFrame of final data
    '''
    
    #check current urls in df
    if 'now':
        start_year = pd.datetime.now().year
        
    # get list of year urls
    year_urls = WHO_year_urls(start_year=start_year)
    
    # get all <a> tags including year and url
    all_articles_tags = articles_a_tag(year_urls)
    
    # get dictionary of date key: list of articles per date value
    articles_dict = make_articles_dict(all_articles_tags)
    
    # download all data
    database = update_WHO(articles_dict, existing_urls)
    
    # convert to DataFrame
    who = pd.DataFrame(database, columns=['time_stamp', 'title', 'url', 'html', 'text', 'source'])
    
    # covert time_stamp to date_time
    who.time_stamp = who.time_stamp.str.slice(0,11)
    who.time_stamp = pd.to_datetime(who.time_stamp)
    
    return who


def obnt_month_urls(start_year='now'):
    '''
    Get list of month urls from outbreaknewstoday.com
    
    start_year: int for specific year 
                str - 'now' or 'all'
        
    Returns
    -------
    list of month urls
    '''
    
    if 'now':
        start_year = pd.datetime.now().year
        
    url = 'http://outbreaknewstoday.com/sitemap.xml'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')
    locs = soup.find('body').findAll('loc')
    
    remove_list = ['http://outbreaknewstoday.com/sitemap-misc.xml', 
                   'http://outbreaknewstoday.com/sitemap-tax-category.xml',
                   'http://outbreaknewstoday.com/sitemap-archives.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2019-12.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2018-01.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2015-02.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2015-01.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2014-08.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2014-05.xml',
                   'http://outbreaknewstoday.com/sitemap-pt-page-2014-04.xml']
    
    month_urls = [i.string for i in locs if i.string not in remove_list]
    
    if start_year != 'all':
        month_urls = [url for url in month_urls if 
                      next(datefinder.find_dates(url)).year == start_year]
    
    return month_urls


def obnt_article_urls(month_url):
    '''
    Get list of article urls per month from outbreaknewstoday.com
    
    Parameters
    ----------
    month_url
    
    Returns
    -------
    dictionary
        key: timestamp
        value: article url    
    '''
    
    r = requests.get(month_url)
    soup = BeautifulSoup(r.content, 'lxml')

    locs = soup.find('body').findAll('loc')
    article_urls = [i.string for i in locs] 

    lastmods = soup.find('body').findAll('lastmod')
    time_stamps = [str(i.string) for i in lastmods]

    article_dict = {time_stamp:url for (url, time_stamp) in zip(article_urls, time_stamps)}
    
    return article_dict


def obnt_all_article_urls(month_urls):
    '''
    Gets full dictionary of article timestamps and urls from outbreaknewstoday.com
    
    Parameters
    ----------
    month_urls
    
    Returns
    -------
    dictionary
        key: timestamp
        value: article url 
    '''
    
    all_urls = {}
    for i in month_urls:
        month_urls = obnt_article_urls(i)
        all_urls.update(month_urls)
           
    return all_urls


def obnt_remove_bad_urls(all_urls):
    '''
    Remove bad urls from all_urls list.
    E.g. 'top 10 lists'    
    '''
    bad_url_strings = ['most-viewed',
                    'stories-of',
                    'top-infectious',
                    'top-10-',
                    'interview']

    bad_url_keys =[]
    
    for i,j in all_urls.items():
        if any(bad_url in str(j) for bad_url in bad_url_strings):
            bad_url_keys.append(i)
                 
    for i in bad_url_keys:
        del all_urls[i]
            
    return all_urls


def obnt_single_article(url):

    url = str(url)
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'lxml')

    data={}
    tag_remove_list = ['img', 'script', 'iframe', 'ins', 'figure']
    string_remove_list = ['<a href="http://outbreaknewstoday',
                          'https://www.youtube.com/channel/UCqjNwMDZ3dElBSB__yW-0sg',
                          'www.cafepress.com',
                          'Outbreak News Merchandise',
                          'Check out what we',
                          'smiledirectclub.com/',
                          'www.vitalsleep.com/',
                          'shareasale.com',
                          'sharesale.com']
    
    # remove specified tags
    for i in tag_remove_list:
        for tag in soup(i):
            tag.decompose()
    
    # main document body
    postsingle = soup.body.find('div', {'class':'postsingle'})

    # get raw html, title and url
    data['html'] = postsingle.prettify(formatter='html')
    data['title'] = soup.title.string
    data['url'] = url

    # find all paragraphs and remove tags from remove list
    p = postsingle.findAll(['p', 'li'])
    text = []
    for i,j in enumerate(p):
        if any(remove_string in str(p[i]) for remove_string in string_remove_list):
            try:
                p[i].a.decompose()
                
                # remove remaining empty tag
                if str(p[i].contents[0]) == '<strong></strong>':
                    p[i].decompose()      
            except:
                p[i].decompose()
                
        for k in j:
            text.append(k.string)  
  
    # join raw strings of separate paragraphs back together
    text = list(filter(None.__ne__, text))
    raw = ' '.join(text)
    data['raw'] = raw
    
    # join html paragraphs back together
    html_text = []
    for i,j in enumerate(p):
        try:
            html_text.append(p[i].prettify(formatter='html'))
        except:
            continue

    html_text = list(filter(None.__ne__, html_text))
    title_text = f'<h1>{soup.title.string}</h1>'

    concise_html = title_text + ''.join(html_text)

    data['concise_html'] = concise_html
    
    return data


def scrape_obnt(articles_dict, existing_urls=[]):
    '''
    Loops through all WHO articles dictionary and downloads data, adding to 'database'
    
    Parameters
    ----------
    dictionary of article urls
    
    Returns
    -------
    cleaned dataframe
    '''
    
    # convert index position to alphabet
    database = {}

    bad_urls = []
    
    bar = progressbar.ProgressBar()

    for time_stamp, url in bar(articles_dict.items()):
        if url not in existing_urls:
            try:
                data = obnt_single_article(url)
                database[time_stamp] = (data['title'], data['url'], data['html'], data['concise_html'], data['raw'])
            except:
                bad_url.append([url, 'obnt', pd.datetime.now()])

    if len(database) > 0:
            
        obnt = pd.DataFrame(database).transpose()
        obnt = obnt.reset_index().rename(columns = {'index' : 'time_stamp'})
        obnt.columns = ['time_stamp', 'title', 'url', 'html_old', 'html', 'text']
        obnt.drop('html_old', axis=1, inplace=True)
        #try:
        obnt.time_stamp = pd.to_datetime(obnt.time_stamp)
        #except:
        #    print(obnt.time_stamp)
        #    return obnt.time_stamp
            


        obnt.time_stamp = obnt.time_stamp.dt.tz_localize(None)
        obnt['source'] = 'obnt'
    else:
        obnt = pd.DataFrame(columns=['time_stamp', 'title', 'url', 'html_old', 'html', 'text'])     
                        
    return obnt, bad_urls


def OBNT_download_new_articles(existing_urls=[]):
    '''
    Downloads all new OBNT articles, excluding specified existing_urls
    Returns DataFrame and list of failed urls
    '''
    month_urls = obnt_month_urls()
    all_urls = obnt_all_article_urls(month_urls)
    obnt, bad_urls = scrape_obnt(all_urls, existing_urls=existing_urls)
    #obnt = scrape_obnt(all_urls, existing_urls=existing_urls)
    
    return obnt, bad_urls   


def CIDRAP_all_urls(max_pages=1184):
    '''
    Gets list of all CIRRAP urls    
    '''
    
    # max page currently at 1183
    page_directory = [f'http://www.cidrap.umn.edu/news-perspective?page={i}' for i in range(0, max_pages)]
    article_urls = []

    bar = progressbar.ProgressBar()

    for url in bar(page_directory):
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html')
        view_content = soup.find_all('div', {'class': 'view-content'})[0]

        for i in view_content.find_all('a'):
            if i.string:
                article_urls.append('http://www.cidrap.umn.edu/' + i['href'])
    
    return article_urls


def cidrap_url_categoriser(article_urls, existing_urls=[]):
    '''
    Separate URLs into single articles and scan pages - which contain multiple articles
    Remove any existing urls    
    Return list of standard and list of scan urls
    '''
    
    cidrap_standard_urls = []
    cidrap_scan_urls = []
    
    for i in article_urls:
        if i not in existing_urls:
            if 'scan' in str(i):
                cidrap_scan_urls.append(i)
            else:
                cidrap_standard_urls.append(i)
            
    return cidrap_standard_urls, cidrap_scan_urls


def cidrap_standard_article(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html')
    
    title = soup.title.string
       
    content = soup.find_all('div', {'class', 'content-inner section'})[0]
    contents = content.find_all(['p', 'h2', 'h3', 'h4', 'h5', 'span'])
    
    for i in contents:
        for tag in i('a'):
            tag.decompose()
    
    data={}
    html=[]
    text=[]

    for j,i in enumerate(contents):        
        if i.name == 'span':
            try:
                i['content']
                time_stamp = i['content']
                continue
            except:
                pass

        else:
            try:
                if 'See also' in str(i.find(['strong', 'b'])):
                    break
         
                else:
                    pass
            except:
                pass
                         
            try:
                html.append(i.prettify(formatter='html'))

                for j in i.stripped_strings:
                    text.append(j)
              
            except:
                pass
            

    html = ' '.join(html)
    text = ' '.join(text)
    
    data['html'] = html
    data['raw'] = text
    try:
        data['time_stamp'] = time_stamp
    except:
        data['time_stamp'] = 'no time_stamp'
    data['url'] = str(url)
    data['title'] = title
    
    return data


def cidrap_scan_article(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html')
    
    title = soup.title.string
       
    content = soup.find_all('div', {'class', 'content-inner section'})[0]
    contents = content.find_all(['p', 'h2', 'h3', 'h4', 'h5', 'span'])
    
    for i in contents:
        for tag in i('a'):
            tag.decompose()
    
    h=0
    data={}
    html=[]
    text=[]
    
    try:
        year = next(datefinder.find_dates(title)).year
    except:
        year = 1900

    for j,i in enumerate(contents):        
        if i.name == 'span':
            try:
                i['content']
                time_stamp = i['content']
                try:
                    year = next(datefinder.find_dates(title)).year
                except:
                    pass
                    
                continue
            except:
                pass

        else:
            try:
                if 'See also' in str(i.find(['strong', 'b'])):
                    break
         
                else:
                    pass
            except:
                pass
            
            if 'h3' in str(i) and i.string != '\xa0':
                               
                h += 1
                
                data[f'title_{h}'] = []
                for m in i.strings:
                    data[f'title_{h}'].append(m)
                data[f'title_{h}'] = ' '.join(data[f'title_{h}'])
                        
            try:
                html.append(i.prettify(formatter='html'))
                if f'html_{h}' not in data:
                    data[f'html_{h}'] = []                
                data[f'html_{h}'].append(i.prettify(formatter='html'))
                
                if i.findAll(['strong', 'b']):
                    try:
                        data[f'date_{h}'] = max([date_ for date_ in datefinder.find_dates(str(i.findAll(['strong', 'b'])))])
                        data[f'date_{h}'] = data[f'date_{h}'].replace(year=year)
                        
                    except:
                        data[f'date_{h}'] = next(datefinder.find_dates(str(title)))

                for j in i.stripped_strings:
                    text.append(j)
                    
                    if f'text_{h}' not in data:
                        data[f'text_{h}'] = []
                    data[f'text_{h}'].append(j)
              
            except:
                pass
            
    for i in range(1,h+1):
        data[f'html_{i}'] = ' '.join(data[f'html_{i}'])
        data[f'text_{i}'] = ' '.join(data[f'text_{i}'])
                  
        
    
    html = ' '.join(html)
    text = ' '.join(text)
    
    data['html'] = html
    data['raw'] = text
    data['headings'] = h
      
    
    try:
        data['time_stamp'] = time_stamp
    except:
        data['time_stamp'] = 'no time_stamp'
        
    data['title'] = title
           
    data['url'] = str(url)
                
    return data


def scan_data_extractor(data):
    database=[]
    
    for i in range(1, data['headings']+1):
        dat={}
        dat['time_stamp'] = data[f'date_{i}']
        dat['html'] = data[f'html_{i}']
        dat['text'] = data[f'text_{i}']
        dat['title'] = data[f'title_{i}']
        dat['url'] = data['url']

        database.append(dat)   
    return database


def scrape_cidrap_standard(cidrap_standard_urls):
    '''
    Scrapes given list of URLs
    Returns formatted DataFrames of results and bad_urls
        
    '''
    urls = cidrap_standard_urls
    database = []
    bad_urls = []

    bar = progressbar.ProgressBar()
    for url in bar(urls):
        try:
            data = cidrap_standard_article(url)
            database.append([data['time_stamp'], data['title'], data['url'], data['html'], data['raw']])
        except:
            bad_urls.append([url, 'cidrap_standard', pd.datetime.now()])

    if len(database) > 0:
        cidrap_standard = pd.DataFrame(database)
        cidrap_standard.columns = ['time_stamp', 'title', 'url', 'html', 'text']
        cidrap_standard.time_stamp = pd.to_datetime(cidrap_standard.time_stamp)
        cidrap_standard.time_stamp = cidrap_standard.time_stamp.dt.tz_localize(None)
        cidrap_standard['source'] = 'cidrap_standard'
        cidrap_standard
    else:
        cidrap_standard = pd.DataFrame(columns=['time_stamp', 'title', 'url', 'html_old', 'html', 'text'])
    
    if len(bad_urls) > 0:
        failed_urls = pd.DataFrame(bad_urls, columns=['bad_urls', 'source', 'time_stamp'])
    else:
        failed_urls = pd.DataFrame(columns=['bad_urls', 'source', 'time_stamp'])
            
    return cidrap_standard, failed_urls


def scrape_cidrap_scan(cidrap_scan_urls):
    '''
    Scrapes given list of URLs
    Returns formatted DataFrames of results and bad_urls
        
    '''
    urls = cidrap_scan_urls
    database = pd.DataFrame(columns=['time_stamp', 'html', 'text', 'title', 'url'])
    bad_urls = []

    bar = progressbar.ProgressBar()
    for url in bar(urls):
        try:
            data = scan_data_extractor(cidrap_scan_article(url))
            database = database.append(pd.DataFrame(data))
        except:
            bad_urls.append([url, 'cidrap_scan', pd.datetime.now()])

    if len(database) > 0:
        #cidrap_scan = pd.DataFrame(database)
        #display(cidrap_scan)
        cidrap_scan = database
        cidrap_scan = cidrap_scan[['time_stamp', 'title', 'url', 'html', 'text']]
        cidrap_scan.time_stamp = pd.to_datetime(cidrap_scan.time_stamp)
        cidrap_scan.time_stamp = cidrap_scan.time_stamp.dt.tz_localize(None)
        cidrap_scan['source'] = 'cidrap_scan'
        cidrap_scan
    else:
        cidrap_scan = pd.DataFrame(columns=['time_stamp', 'title', 'url', 'html_old', 'html', 'text'])

    if len(bad_urls) > 0:
        failed_urls = pd.DataFrame(bad_urls, columns=['bad_urls', 'source', 'time_stamp'])

    else:
        failed_urls = pd.DataFrame(columns=['bad_urls', 'source', 'time_stamp'])
        
    return cidrap_scan, failed_urls


def cidrap_download_new_articles(existing_urls=[], old_failed=[]):
    '''
    Downloads all new CIDRAP articles, excluding specified existing_urls
    Returns DataFrame and list of failed urls
    '''
    article_urls = CIDRAP_all_urls(max_pages=12)
    article_urls.extend(old_failed)
    cidrap_standard_urls, cidrap_scan_urls = cidrap_url_categoriser(article_urls, existing_urls=existing_urls)
    cidrap_standard, bad_urls_standard = scrape_cidrap_standard(cidrap_standard_urls)
    cidrap_scan, bad_urls_scan = scrape_cidrap_scan(cidrap_scan_urls)
        
    return cidrap_standard, cidrap_scan, bad_urls_standard, bad_urls_scan 


if __name__ == '__main__':
        main()
       

