'''查找MongoDB数据库，看看哪些日期缺失了数据
比如网站上显示这一天有300条数据，而数据库中只有280条，则后续要补录这一天的数据'''
from datetime import date, timedelta
import pymongo
from logger import logger
from spider import date_range, get_response, get_page_nums


# 连接MongoDB
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.aqi
collection = db.aqi


if __name__ == '__main__':
    # 获取起始日期到今天为止的所有日期列表(生成器)
    gen_dates = date_range(date(2014, 1, 1), date.today(), timedelta(days=1))

    problem_dates = []
    for d in gen_dates:
        # 先获取总记录数和总页数，只需要请求第一页数据即可
        resp = get_response(d, 1)
        record_nums, page_nums = get_page_nums(resp)
        # 查询MongoDB中当前日期的记录数，找出那些与record_nums不一致的日期（如果那一天没有数据，record_nums和数据库中都是0，还是一致的）
        if collection.find({'date': d}).count() != record_nums:
            logger.info('Date [{}] has problem, please check it then'.format(d))
            problem_dates.append(d)
