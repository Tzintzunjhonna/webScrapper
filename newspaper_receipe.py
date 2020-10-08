from urllib.parse import urlparse
import pandas as pd
import argparse
import logging
import hashlib
import nltk
from nltk.corpus import stopwords

from pandas.core.dtypes import missing

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def main(filename):
    logger.info('Comienza limpiando el proceso')
    df = _read_data(filename)
    newspaper_uid = _extract_newspaper_uid(filename)
    df = _add_newspaper_uid_column(df, newspaper_uid)
    df = _extract_host(df)
    df = _file_missing_titles(df)
    df = _generate_uids_for_rows(df)
    df = _remove_new_lines_from_body(df)
    df = _contador_de_palabras_body_tittle(df)
    return df


def _read_data(filename):
    logger.info('Leyendo el archivo {}' .format(filename))
    return pd.read_csv(filename)


def _extract_newspaper_uid(filename):
    logger.info('Extrayendo Newspaper uid')
    newspaper_uid = filename.split('_')[0]

    logger.info('Newspaper ha sido detectado{}' .format(newspaper_uid))
    return newspaper_uid


def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info(
        'Llenaremos la columna newspaper_uid con {}' .format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid

    return df


def _extract_host(df):
    logger.info('Extrayendo el Host de las urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df


def _file_missing_titles(df):
    logger.info('Filling missing titles')
    missing_titles_mask = df['title'].isna()
    missing_titles = (df[missing_titles_mask]['url']
                      .str.extract(r'(?P<missing_titles>[^/]+)$')
                      .applymap(lambda title: title.split('_'))
                      .applymap(lambda tittle_word_list: ' '.join(tittle_word_list))
                      )
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

    return df


def _generate_uids_for_rows(df):
    logger.info('Generando UIDS para each row')
    uids = (df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
            .apply(lambda hash_object: hash_object.hexdigest())
            )

    df['uid'] = uids
    return df.set_index('uid')


def _remove_new_lines_from_body(df):
    logger.info('Removiendo nuevas lineas del body')

    stripped_body = (df
                .apply(lambda row: row['body'], axis=1)
                .apply(lambda body: list(body))
                .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ' '), letters)))
                .apply(lambda letters: ''.join(letters))
    )
    
    df['body'] = stripped_body

    return df

def contador_de_palabras_body_tittle(df, column_name): #Función que corre para realizar conteo
    logger.info('Contando palabras')

    stop_words = set(stopwords.words('spanish'))
    return (df
           .dropna()
            .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
            .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
            .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
            .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
            .apply(lambda valid_word_list: len(valid_word_list))
           )

def _contador_de_palabras_body_tittle(df): # Función que se manda a traer para mostrar

    df['# palabras titulo'] = contador_de_palabras_body_tittle(df, 'title')
    df['# palabras cuerpo'] = contador_de_palabras_body_tittle(df, 'body')
    
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The phat a los datos sucios',
                        type=str)
    args = parser.parse_args()
    df = main(args.filename)
    print(df)
