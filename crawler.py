import requests as r
from multiprocessing import Pool
from bs4 import BeautifulSoup
from hashlib import md5
import re
import os
import mysql.connector


db = mysql.connector.connect(host='', user='', password='', database='')
sql = db.cursor()
s = r.Session()


def download(url, file_path, ext):
    con = s.get(url).content
    if str(con).find('<?xml') >= 0:
        return
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    path = '{0}/{1}.{2}'.format(file_path, md5(con).hexdigest(), ext)
    if not os.path.exists(path):
        with open(path, 'wb') as f:
            f.write(con)
            f.close()


def create_table():
    insert = "CREATE TABLE `Store`.`tumblr` (`id` INT NOT NULL AUTO_INCREMENT," \
        "`author` VARCHAR(45) NULL,`body` TEXT NULL,`video` TEXT NULL,`photo` " \
            "TEXT NULL,PRIMARY KEY (`id`),UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE);"
    sql.execute(insert)
    db.commit()


def save_to_db(d):
    insert = "INSERT INTO tumblr (author, body, video, photo) VALUES (%s,%s,%s,%s)"
    sql.execute(insert, (str(d['a']), d['b'], str(d['v']), str(d['p'])))
    db.commit()
    print('Succeed saving to MySql')


def parse(posts, page):
    path = '' #fill in local path
    for p in posts:
        item = {}
        try:
            person = p.find('div', class_="post_info").a['data-peepr'].split('"')[3]
        except AttributeError:
            return
        #print(person)
        item['a'] = person
        body = p.find('div', class_="post_body")
        reblog = p.find('div', class_="reblog-content")
        #print(body)
        #print(reblog)
        if reblog:
            reblog = reblog.find_all('p')
            item['b'] = str(reblog)
        elif body:
            item['b'] = str(body)
        else:
            item['b'] = 'None'
        vid = p.find('video')
        pho = p.find('div', class_="photoset")
        #print(vid)
        #print(pho)
        try:
            item['v'] = vid.source['src']
            download(item['v'], '{0}/{1}'.format(path, item['a']), 'mp4')
        except AttributeError:
            item['v'] = None
        try:
            pho = pho.find_all('a')
            for j, ph in enumerate(pho):
                pho[j] = ph['href']
                download(pho[j], '{0}/{1}'.format(path, item['a']), 'jpg')
            item['p'] = pho
        except AttributeError:
            item['p'] = None
        if (item['v'] is None and item['p'] is None) or len(item['b']) >= 4000:
            item['b'] = '{0}th'.format(page)
        print(item)
        save_to_db(item)


def get_data(start, end):
    for i in range(start, end):
        url = 'https://www.tumblr.com/likes/page/{0}/'.format(i)
        print('{} th page'.format(i))
        ith_page = s.get(url)
        soup = BeautifulSoup(ith_page.text, features="html.parser")
        try:
            posts = soup.find('ol', class_="posts").find_all('li')
            parse(posts, i)
        except AttributeError:
            print('{}th has nothing'.format(i))


def login(email, password, url):
    header = {"User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/600.7.12 '
                            '(KHTML, like Gecko) Version/8.0.7 Safari/600.7.12'}
    response = s.get(url, headers=header)
    hidden = re.findall('(<input.*?/>)', re.findall('"form_row_hidden">(.*?)</div>', response.text, re.S)[0])
    key = {}
    for i in hidden:
        key.update({i.split('name="')[1].split('"')[0]: i.split('value="')[1].split('"')[0]})
    key.update({'determine_email': email,
                'user[email]': email,
                'user[password]': password,
                'user[age]': '',
                'tumblelog[name]': ''})
    s.post(url, data=key, headers=header)


def main(start):
    create_table()
    login('', '', "https://www.tumblr.com/login?redirect_to=%2Flikes#")#fill email, password
    get_data(start, start+10)
    sql.close()
    db.close()


if __name__ == '__main__':
    pool = Pool()
    pool.map(main, [i*10 for i in range(25)])
    #main(254)
{"mode":"full","isActive":false}