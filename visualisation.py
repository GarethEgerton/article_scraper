import streamlit as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from PIL import Image

from make_dataset import articles_database
from TFIDF import spacy_tfidf 


def title_cleaner(news_title):
    title_stopwords = ['Outbreak News Today', 'CIDRAP', ' -', ' |']
    for i in title_stopwords:
        news_title = news_title.replace(i, '')  
    return news_title


def source_cleaner(news_source):
    cidrap_replace = ['_STANDARD', '_SCAN']
    for i in cidrap_replace:
        news_source = news_source.replace(i, '')  
    return news_source    


def summary_type_chooser(i):
    # use BERT 
    if i[1][5]:
        return i[1][5]
      
    # use TFIDF
    else:
        return spacy_tfidf(i[1][4])  


def bert_join(df):
    '''
    Returns df with joined bert summaries column    
    '''
    bert_df = pd.read_csv('bert_data.csv').drop('Unnamed: 0', axis=1)
    bert_df.set_index('index', inplace=True)
    bert_df.drop('text', inplace=True, axis=1)
    bert_joined = df.join(bert_df)
    
    return bert_joined


image = Image.open('virus5.png')
st.image(image, use_column_width=True)

#st.title('Too many outbreaks..... poooooaaaar')

#st.text('This is some text.')



df = articles_database()
df = bert_join(df)

#data_load_state = st.text('Loading data...')

data = df

#data_load_state.text('Loading data... done')

#st.subheader('Raw data')
#st.write(data.text)


# filters
date_low = '2019-oct-01'
date_high = '2020-feb-20'

country = ''
sort_by = ['time_stamp', 'source']
num_results = 10

st.sidebar.subheader('Infectious Disease News Search')

date_low = st.sidebar.text_input('start date', '2019-oct-01')

date_high = st.sidebar.text_input('end date', '2020-feb-20')

disease = st.sidebar.radio(
    "Select disease: ",
    ('Corona', 
    'Yellow fever', 
    #'Flu',
    'Ebola',
    #'Malaria',
    'Cholera',
    #'Dengue',
    'Zika',))
    #'Japanese Encephalitis'))

country = st.sidebar.text_input('Country: ', '')


all_summaries_needed = ['Compare all', 'Rate summaries']

summary_selector = st.sidebar.radio(
    "Summary type: ",
    ('Full article',
    'TFIDF', 
    'BERT', 
    'Compare all',
    'Rate summaries'))

if summary_selector == 'TFIDF' or summary_selector in all_summaries_needed:
    tfidf_proportion = st.sidebar.slider('TFIDF Summary percentrage', min_value=0, max_value=100, value=30) /100

if summary_selector != 'Rate summaries':
    num_results = st.sidebar.number_input('Max number results:', min_value=1, max_value=200, value=10)
else:
    num_results=1

#news_source = st.sidebar.multiselect('News Source', ['WHO', 'OBNT', 'CIDRAP'])


#st.write(f'You selected {disease}')


search_results = df[(df.text.str.lower().str.contains(disease.lower())) &
                    (df.time_stamp < date_high) &
                    (df.time_stamp > date_low) &
                    (df.text.str.lower().str.contains(country.lower())
                      )].copy()

if num_results > search_results.shape[0]:
    results_len = search_results.shape[0]
else:
    results_len = num_results

search_results.sort_values(sort_by, ascending=False, inplace=True)

print(f'Total articles:    {search_results.shape[0]}')
print(search_results.source.value_counts())

try: 
    plt.figure(figsize=(20, 10))
    #plt.plot(merge_results.date_normalized, merge_results.views)
    plt.hist(search_results.time_stamp, bins=30, label='ALL', alpha=0.5)
    plt.hist(search_results[search_results.source=='obnt'].time_stamp, bins=30, label='obnt', alpha=.7)
    plt.hist(search_results[search_results.source=='cidrap_scan'].time_stamp, bins=30, label='cidrap_scan', alpha=.7)
    plt.hist(search_results[search_results.source=='cidrap_standard'].time_stamp, bins=30, label='cidrap_standard', alpha=.8)
    plt.hist(search_results[search_results.source=='who'].time_stamp, bins=30, label='who', alpha=1)

    plt.legend()

    plt.show()

    st.pyplot()

except:
    st.error('Error displaying graph')

#display(HTML(f'<h5>- Top {results_len} articles:</h5>'))

html_doc = []

date_set = set()
year_set = set()

import time

my_bar = st.progress(0)
bar_length = len(search_results[0:num_results])

if st.sidebar.button('Search'):
    with st.spinner(text='Calculating TFIDF in real time...'):

        for j,i in enumerate(search_results[0:num_results].iterrows()):
            
            date = i[1][0]
            date_str = i[1][0].strftime('%d %b')
            year_str = i[1][0].strftime('%Y')
            
            if date_str not in date_set:
                date_display = f'<h3><b>{date_str}</b></h3>'
            else:
                date_display = ''
            
            if year_str not in year_set:
                year_display = f'<h1><b>{year_str}</b></h1>'
            else:
                year_display = ''
            
            if len(year_display) > 0:
                year_rule = '<hr align="left", width="100%">'
            else:
                year_rule = ''

            news_title = title_cleaner(i[1][1])
            news_source = source_cleaner(i[1][5].upper())

            all_summaries_needed = ['Compare all', 'Rate summaries']


            if summary_selector == 'TFIDF' or summary_selector in all_summaries_needed :
                tfidf_summary = spacy_tfidf(i[1][4], proportion=tfidf_proportion)
                summary_output = tfidf_summary
            
            if summary_selector == 'BERT' or summary_selector in all_summaries_needed :
                bert_summary = i[1][6]
                summary_output = bert_summary 
            
            if summary_selector == 'Full article' or summary_selector in all_summaries_needed:
                full_article = i[1][4]
                summary_output = full_article 

            

            #summary_type = {'TFIDF': tfidf_summary,
            #                'BERT': bert_summary,
            #                'Full article': full_article,
            #                'Compare all': True}

            #summary_output = summary_type[summary_selector]         

            if summary_selector not in all_summaries_needed:
                article_html = ''.join([f'{year_display}{year_rule}', f'{date_display}', f'<h4>{news_title} - <a href="{i[1][2]}", target="_blank">{news_source}  </a></h4>', 
                                        f'<BLOCKQUOTE>{summary_output}</BLOCKQUOTE>'])
            elif summary_selector == 'Rate summaries':
                article_html = ''.join([f'{year_display}{year_rule}', f'{date_display}', f'<h4>{news_title} - <a href="{i[1][2]}", target="_blank">{news_source}  </a></h4>', 
                                    f'<BLOCKQUOTE><b><font color="red">RANDOM SUMMARY: </font></b>{np.random.choice([bert_summary, tfidf_summary])}</BLOCKQUOTE>', 
                                    f'<BLOCKQUOTE><b><font color="red">FULL TEXT: </font></b>{full_article}</BLOCKQUOTE>'])
            else:
                article_html = ''.join([f'{year_display}{year_rule}', f'{date_display}', f'<h4>{news_title} - <a href="{i[1][2]}", target="_blank">{news_source}  </a></h4>', 
                                    f'<BLOCKQUOTE><b><font color="red">BERT SUMMARY: </font></b>{bert_summary}</BLOCKQUOTE>', f'<BLOCKQUOTE><b><font color="red">TFIDF SUMMARY: </font></b>{tfidf_summary}</BLOCKQUOTE>', f'<BLOCKQUOTE><b><font color="red">FULL TEXT: </font></b>{full_article}</BLOCKQUOTE>'])
            

            
            
            
            



            date_set.add(date_str)
            year_set.add(year_str)
            
            html_doc.append(article_html)

            my_bar.progress((j+1)/bar_length)

        #st.sidebar.success('Complete')

html_output = ''.join(html_doc)

#display(HTML(html_output)) 
#display(HTML(f'<h5>Results:</h5>'))

st.markdown(html_output, unsafe_allow_html=True)

if summary_selector == 'Rate summaries':
    st.subheader('Questionnaire')
    st.text('1. To what extent does the summary cover the key information in the text (1-5)?')

    st.radio('Select:', 
            options = ['1 – Irrelevant info provided – misses main point',
                       '2 – Basic information – Not much more information than conveyed in title.',
                       '3 – Some key points – Title information plus some further relevant.',
                       '4 – Key points – coveys around 60% essence of content',
                       '5 – Most key points – conveys 80% or more essence of content'])
    st.text('')
    st.text('2. Is there any key information missing that should have been included?')
    st.text_area('Paste any critical missing sentences: ')

    st.text('')
    st.text('3. Is there any superfluous information that was unnecessary to include to cover the essence of the news article?')
    st.text_area('Paste unnecessary included sentences: ')

    st.text('')
    st.text('4. To what extent does the summarised information flow from one sentence to the next?')
    st.radio('Select:', 
            options = ['1 – Disjointed and seemingly random',
                       '2 – Poor – feels like information is missing, not following logical flow.',
                       '3 – Moderately – Small feeling of missing information',
                       '4 – Well  - Flows but clear that it’s a summarised version.',
                       '5 – Very well, does not seem like it’s a summary. Could be a short article in its own right.'])

    st.text('')
    st.text('5. Is there any possibility for misinformation if reading only a summary?')
    st.radio('Select:', 
            options = ['1 – Yes – conveys a false idea',
                       '2 – Likely gives wrong impression',
                       '3 – Potential to convey wrong impression',
                       '4 – Very unlikely',
                       '5 -  Not at all'])

    st.text('')
    st.text('Imagine you have only 5 minutes to read the 10 articles provided.')
    st.text('6. To what extent do you think that reading the shorthand version of the article would be beneficial vs the whole article?')
    st.radio('Select:', 
            options = ['1 – No benefit, prefer to skim read the full article myself',
                       '2 – Some benefits but not much.',
                       '3 – Would be more effective but might miss some useful information',
                       '4 – Yes would be faster, and cover most key points',
                       '5 - Yes, provides all needed information efficiently in time available.'])


    if st.button('Submit'):
        st.balloons()