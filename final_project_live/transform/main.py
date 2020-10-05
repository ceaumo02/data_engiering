import argparse
import hashlib
import logging
import re
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import nltk
from nltk.corpus import stopwords
import pandas as pd


stop_words = set(stopwords.words('spanish'))
logger = logging.getLogger(__name__)


def main(filename):
    logger.info('Starting cleaning process')

    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df =_add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _fill_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_new_lines_form_body(df)
    df = _data_enrichment(df)
    df = _remove_all_duplicates(df)
    df = _drop_rows_with_missing_values(df)
    df = _save_data(df, filename)
    # news_name = filename.split('_')
    # df.to_csv(f'{news_name[0]}_limpio.csv', encoding='utf-8', sep= ';')

    return df

def _read_data(filename):
    logger.info(f'reading file {filename}')

    return pd.read_csv(filename, encoding= 'latin-1')


def _extract_newspaper_uid(filename):
    logger.info(f'Extracting newspaper uid')
    newspaper_uid = filename.split('_')[0]

    logger.info(f'Newspaper uid detected: {newspaper_uid}')
    return newspaper_uid


def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info(f'Filling newspaper_uid column with {newspaper_uid}')
    df['newspaper_uid'] = newspaper_uid

    return df  


def _extract_host(df):
    logger.info(f'Extracting host from url')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df


def _fill_missing_titles(df):
    logger.info('Filling missing titles')
    missing_titles_mask = df['title'].isna()

    missing_titles = (df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.split('-'))
                        .applymap(lambda title_word_list: ' '.join(title_word_list)))

    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

    return df


def _generate_uids_for_rows(df):
    logger.info('Generating uids for each row')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest()))

    df['uid'] = uids

    return df.set_index('uid')


def _remove_new_lines_form_body(df):
    logger.info('Removing new lines from body')

    stripped_body = (df
                        .apply(lambda row: row['body'], axis=1)
                        .apply(lambda body: re.sub(r'(\n|\r)+', r'', body)))

    df['body'] = stripped_body
    return df


def _data_enrichment(df):

    def _tokenize_column(df, column_name):
        logger.info(f'Tokenizing title and {column_name}')
        return (df
                    .dropna()
                    .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
                    .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                    .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                    .apply(lambda words_list: list(filter(lambda word: word not in stop_words, words_list)))
                    .apply(lambda valid_word_list: len(valid_word_list)))

    df[f'n_tokens_title'] = _tokenize_column(df, 'title')
    df[f'n_tokens_body'] = _tokenize_column(df, 'body')

    return df


def _remove_all_duplicates(df):
    def _remove_duplicate_entries(df, column_name):
        logger.info('Removing duplicates entries')
        df.drop_duplicates(subset=[column_name], keep='first', inplace=True)

        return df

    _remove_duplicate_entries(df, 'title')
    _remove_duplicate_entries(df, 'body')
    _remove_duplicate_entries(df, 'url')

    return df


def _drop_rows_with_missing_values(df):
    logger.info(('Dropping rows with missing values'))

    return df.dropna()


def _save_data(df, filename):
    clean_filename = f'Clean_{filename}'
    logger.info(f'Saving data at location: {clean_filename}')
    df.to_csv(clean_filename, encoding='utf-8')

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The path to the dirty data',
                        type=str)

    args = parser.parse_args()

    df = main(args.filename)
    print(df)