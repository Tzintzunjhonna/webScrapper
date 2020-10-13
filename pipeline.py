import logging
logging.basicConfig(level=logging.INFO)
import subprocess
import datetime



logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais']


def main():
    _extraer()
    _transfomar()
    _cargar()
    _finalizacion()


def _extraer():
    logger.info('Comenzando con el proceso de extracción')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = f'{news_site_uid}_{now}_articles.csv'
        subprocess.run(['python', 'main.py', news_site_uid], cwd='./extraer')

def _transfomar():
    logger.info('Comenzando con el proceso de transformación')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = f'{news_site_uid}_{now}_articles.csv'
        clean_data_filename = f'clean_{dirty_data_filename}'
        subprocess.run(f'MOVE extraer\{dirty_data_filename} transformar', shell=True)
        subprocess.run(['python', 'main.py', f'{dirty_data_filename}'], cwd='./transformar')


def _cargar():
    logger.info('Comenzando con la carga a base de datos')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = f'{news_site_uid}_{now}_articles.csv'
        clean_data_filename = f'clean_{dirty_data_filename}'
        subprocess.run(f'DEL transformar\{dirty_data_filename}', shell=True)
        subprocess.run(f'MOVE transformar\{clean_data_filename} cargar', shell=True)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./cargar')

def _finalizacion():
    logger.info('Comenzando con la carga a base de datos')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = f'{news_site_uid}_{now}_articles.csv'
        clean_data_filename = f'clean_{dirty_data_filename}'
        subprocess.run(f'DEL transformar\{clean_data_filename}', shell=True)
        subprocess.run(f'DEL cargar\{clean_data_filename}', shell=True)


    logger.info('*************************************************')
    logger.info('La sistematización termino con exito')

if __name__ == '__main__':
    main()