from summarizer import Summarizer
from src.data.make_dataset import articles_database
import pandas as pd
import numpy as np
import progressbar


model = Summarizer()


def main():
    bert_new_article_summary_pipeline()
    

def bert_summarizer(doc, model=model, min_length=60):

    '''
    Given document and model, returns summary
    '''
    result = model(doc, min_length=60)
    full = ''.join(result)
    return full  


def bert_join(df):
    '''
    Returns df with joined bert summaries column    
    '''
    bert_df = pd.read_csv('../../data/processed/bert_data.csv').drop('Unnamed: 0', axis=1)
    bert_df.set_index('index', inplace=True)
    bert_df.drop('text', inplace=True, axis=1)
    bert_joined = df.join(bert_df)
    
    return bert_joined


def bert_summaries_oustanding(df):
    '''
    Returns integer of new bert summaries to complete   
    '''
    summaries_to_complete = 0
    
    for i in df.bertmin60[::-1]:
        if i is np.nan:
            summaries_to_complete += 1
        else:
            break

    return summaries_to_complete


def update_recent_summaries(df, num):
    '''
    Summarises any new articles newest to oldest that have yet to be summarised   
    
    Parameters -
    num - number of summaries needed    
    '''
        
    indexes = list(df.text[::-1].index[:num])
    
    bar = progressbar.ProgressBar()

    failed_summaries=[]

    interim_summaries = pd.read_csv(f'../../data/processed/bert_data.csv').drop('Unnamed: 0', axis=1)
    existing_bertmin60 = set(interim_summaries['index'])

    for i in bar(indexes):

        try:
            if i not in existing_bertmin60:
                row = pd.DataFrame([[i, df.text[i], bert_summarizer(df.text[i], min_length=60)]], columns=['index', 'text', 'bertmin60'])
                row.to_csv(f'../../data/processed/bert_data.csv', mode='a', header=False)    
        except:
            failed_row = pd.DataFrame([i])
            failed_row.to_csv('../../data/processed/failed_bert_indexes.csv', mode='a', header=False)   


def bert_update_recent(df, num_summaries):
    '''
    Summarises any new articles newest to oldest that have yet to be summarised   
    
    Parameters -
    num - number of summaries needed    
    '''
        
    indexes = list(df.text[::-1].index[:num_summaries])
    
    bar = progressbar.ProgressBar()

    failed_summaries=[]

    interim_summaries = pd.read_csv(f'../../data/processed/bert_data.csv').drop('Unnamed: 0', axis=1)
    existing_bertmin60 = set(interim_summaries['index'])

    for i in bar(indexes):

        try:
            if i not in existing_bertmin60:
                row = pd.DataFrame([[i, df.text[i], bert_summarizer(df.text[i], min_length=60)]], columns=['index', 'text', 'bertmin60'])
                row.to_csv(f'../../data/processed/bert_data.csv', mode='a', header=False)    
        except:
            failed_row = pd.DataFrame([i])
            failed_row.to_csv('../../data/processed/failed_bert_indexes.csv', mode='a', header=False)   


def bert_new_article_summary_pipeline():
    '''
    Compares current database to bert_data, updates any new summaries    
    '''
    df = articles_database()
    bert_joined = bert_join(df)
    num_summaries = bert_summaries_oustanding(bert_joined)
    bert_update_recent(bert_joined, num_summaries)    


if __name__ == '__main__':
        main()
       