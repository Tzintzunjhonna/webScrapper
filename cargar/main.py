import argparse
import logging
logging.basicConfig(level=logging.INFO)

import pandas as pd
from article import Article
from base import Base, engine, Session

logger = logging.getLogger(__name__)


def main(filename):
    Base.metadata.create_all(engine)
    session = Session()
    articles = pd.read_csv(filename)

    for index, row in articles.iterrows():
        logger.info('Cargando uid del articulo {} dentro de la base de datos'.format(row['uid']))
        article = Article(row['uid'],
                        row['body'],
                        row['host'],
                        row['newspaper_uid'],
                        row['# palabras cuerpo'],
                        row['# palabras titulo'],
                        row['title'],
                        row['url'],
                        )
        session.add(article)

    session.commit()
    session.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='El archivo que quieres cargar a la base de datos',
                        type=str)

    args = parser.parse_args()

    main(args.filename)