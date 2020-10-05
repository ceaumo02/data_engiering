import logging
logging.basicConfig(level=logging.INFO)
import subprocess
import datetime


logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais', 'eltiempo'] # 'eluniversal', 'elpais',


def main():
    _extract()
    _transform()
    _load()


def _extract():
    logger.info('Starting extract process')
    for news_site_uid in news_sites_uids:
        subprocess.run(['python', 'main.py', news_site_uid], cwd='./extract')
        subprocess.run(['copy', r'C:\Python\final_project_live\extract\*.csv',
        r'C:\Python\final_project_live\transform'], shell=True)


def _transform():
    logger.info('Starting transform process')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = '{}_{datetime}_articles.csv'.format(news_site_uid, datetime=now)
        subprocess.run(['python', 'main.py', dirty_data_filename], cwd='./transform')
        subprocess.run(['del', dirty_data_filename], shell=True, cwd='./transform')      
        subprocess.run(['copy', r'C:\Python\final_project_live\transform\*.csv',
        r'C:\Python\final_project_live\load'], shell=True)
        
def _load():
    logger.info('Starting load process')
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    for news_site_uid in news_sites_uids:
        clean_data_filename = 'clean_{}_{datetime}_articles.csv'.format(news_site_uid, datetime=now)
        subprocess.run(['python', 'main.py', clean_data_filename], cwd='./load')
        # subprocess.run(['del', 'clean_{}_{datetime}_articles.csv'.format(news_site_uid, datetime=now)], shell=True, cwd='./load')

if __name__ == '__main__':
    main()