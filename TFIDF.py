import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from string import punctuation

extra_words = list(STOP_WORDS) + list(punctuation) + ['\n']
nlp = spacy.load('en_core_web_sm')

def main(doc):
    spacy_tfidf(doc, nlp)


def spacy_tfidf(doc, proportion=0.3):
    '''
    Performs basic TFIDF title creation and document summary
    '''
    doc_spacy = nlp(doc)
    all_words = [word.text for word in doc_spacy]
    freq_word = {}
    for w in all_words:
        w = w.lower()
        if w not in extra_words and w.isalpha():
            if w in freq_word.keys():
                freq_word[w]+=1
            else:
                freq_word[w]=1

    val = sorted(freq_word.values())
    max_freq = val[-3:]
    most_frequent_words = []

    for word, freq in freq_word.items():
        if freq in max_freq:
            most_frequent_words.append(word)
        else:
            continue

    for word in freq_word.keys():
        freq_word[word] = (freq_word[word] / max_freq[-1])

    sent_strength={}
    for sentence in doc_spacy.sents:
        for word in sentence:
            if word.text.lower() in freq_word.keys():
                if sentence in sent_strength.keys():
                    sent_strength[sentence] += freq_word[word.text.lower()]
                else:
                    sent_strength[sentence] = freq_word[word.text.lower()]
            else: 
                continue

    top_sentences = (sorted(sent_strength.values())[::-1])
    toppercent_sentence = int(proportion*len(top_sentences))
    if toppercent_sentence < 1:
        toppercent_sentence = 1

    summary=[]
    for sentence, strength in sent_strength.items():
        if strength in top_sentences[0:toppercent_sentence]:
            summary.append(sentence)
        else:
            continue
           
    summary_str = ' '.join([str(i) for i in summary])
    
    return summary_str


if __name__ == '__main__':
        main()