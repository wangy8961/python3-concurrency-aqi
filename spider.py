import time
from datetime import datetime, date, timedelta
import pymongo
import re
from concurrent import futures
import requests
from bs4 import BeautifulSoup
from logger import logger


# 连接MongoDB
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.aqi
collection = db.aqi


def get_response(v_date, page_num):
    '''获取全国城市空气质量日报数据的某一分页'''
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'http://datacenter.mep.gov.cn',
        'Referer': 'http://datacenter.mep.gov.cn/websjzx/report/list.vm',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36'
    }
    data = {
        'V_DATE': v_date,
        'pageNum': page_num,
        'orderby': '',
        'ordertype': '',
        'xmlname': '1512478367400',
        'gisDataJson': '',
        'queryflag': 'close',
        'customquery': 'false',
        'isdesignpatterns': 'false',
        'roleType': 'CFCD2084',
        'permission': '0',
        'AREA': '',
        'inPageNo': 1
    }

    # 捕获request.get方法的异常，比如连接超时、被拒绝等
    try:
        resp = requests.post('http://datacenter.mep.gov.cn/websjzx/report/list.vm', data, headers=headers)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        # In the event of the rare invalid HTTP response, Requests will raise an HTTPError exception (e.g. 401 Unauthorized)
        logger.error('HTTP Error: %s', errh)
        return
    except requests.exceptions.ConnectionError as errc:
        # In the event of a network problem (e.g. DNS failure, refused connection, etc)
        logger.error('Connecting Error: %s', errc)
        return
    except requests.exceptions.Timeout as errt:
        # If a request times out, a Timeout exception is raised. Maybe set up for a retry, or continue in a retry loop
        logger.error('Timeout Error: %s', errt)
        return
    except requests.exceptions.TooManyRedirects as errr:
        # If a request exceeds the configured number of maximum redirections, a TooManyRedirects exception is raised. Tell the user their URL was bad and try a different one
        logger.error('Redirect Error: %s', errr)
        return
    except requests.exceptions.RequestException as err:
        # catastrophic error. bail.
        logger.error('Else Error: %s', err)
        return

    return resp


def get_page_nums(resp):
    # 如果resp.text中存在 暂无数据，表示当天没有数据
    if re.match(r'.*?<div class="report_page_null">暂无数据', resp.text, re.S):
        return 0, 0

    # 如果get_page_nums为True，表示是第一次请求，来获取当天的总记录数和总页数
    m = re.match(r'.*?总记录数.*?(\d+).*?条.*?总页数.*?(\d+).*?</div>', resp.text, re.S)
    record_nums = int(m.group(1))  # 总记录数
    page_nums = int(m.group(2))  # 总页数
    return record_nums, page_nums


def get_page_data(resp):
    '''获取每个分页中的10行数据'''
    soup = BeautifulSoup(resp.text, 'lxml')
    tr_tags = soup.find('table', {'class': 'report-table'}).find_all('tr')
    for tr_tag in tr_tags[1:]:  # 排除表头那一行
        yield {
            'city': tr_tag.find('td', {'colid': 3}).get_text(),  # 城市
            'aqi': tr_tag.find('td', {'colid': 4}).get_text(),  # AQI指数
            'contaminant': tr_tag.find('td', {'colid': 5}).get_text(),  # 首要污染物
            'date': tr_tag.find('td', {'colid': 6}).get_text(),  # 日期
            'level': tr_tag.find('td', {'colid': 8}).get_text(),  # 空气质量级别
        }


def get_one_day_data(date):
    '''获取某一天的全国城市空气质量日报数据'''
    # 访问太频繁的话，会被封IP
    time.sleep(1)
    # 先获取总记录数和总页数，只需要请求第一页数据即可
    resp = get_response(date, 1)
    record_nums, page_nums = get_page_nums(resp)

    # 如果请求当前日期的第1页，返回的record_nums为0，表示这一天全国没有数据
    if record_nums == 0:
        logger.info('Date [{}] has no data'.format(date))
        return

    # 如果有数据
    if record_nums > 0:
        # 先查询MongoDB中当前日期的记录数，如果与record_nums相同，则不需要再下载
        if collection.find({'date': date}).count() == record_nums:
            logger.info('Date [{}] has all data exist in MongoDB, ignore download'.format(date))
            return

        # 循环请求每一分页的数据
        for page_num in range(1, page_nums + 1):
            logger.info('Get date [{}] No.{} page data'.format(date, page_num))
            # 访问太频繁的话，会被封IP
            time.sleep(1)
            resp = get_response(date, page_num)
            for item in get_page_data(resp):
                # 如果该条记录已存在，则不存储到数据库
                if not collection.find_one(item):
                    if collection.insert_one(item):
                        logger.debug('Successfully Saved [{}] to MongoDB'.format(item))
                    else:
                        logger.debug('Error saving [{}] to MongoDB'.format(item))
                else:
                    logger.debug('Record [{}] has exist in MongoDB'.format(item))


def date_range(start, stop, step):
    '''返回生成器： 使用标准的数学和比较操作符来运算日期和时间，返回日期范围(已转换为字符串形式)'''
    while start < stop:
        yield datetime.strftime(start, '%Y-%m-%d')
        start += step


def get_many_days_data():
    '''多线程下载，每个线程获取一天的数据'''
    # 获取起始日期到今天为止的所有日期列表(生成器)
    gen_dates = date_range(date(2018, 8, 1), date.today(), timedelta(days=1))

    workers = 10
    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(get_one_day_data, gen_dates)

    return len(list(res))


if __name__ == '__main__':
    t0 = time.time()
    count = get_many_days_data()
    msg = 'Download {} days AQI data in {} seconds.'
    logger.info(msg.format(count, time.time() - t0))
