import os, warnings, sys, MySQLdb, logging
from countryGuesser import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


cg = CountryGuesser()


def create_table(cur, tableName):
    # whether the table exists
    try:
        cur.execute("select max(id) from {}".format(tableName))
        cur.fetchone()
        exists = True
    except Exception as e:
        exists = False
    if exists == False:
        sql = "CREATE TABLE `" + tableName + "` (" \
                  "`id` int(11) NOT NULL AUTO_INCREMENT, " \
                  "`user_id` int(11) NOT NULL, " \
                  "`country` varchar(255) DEFAULT NULL, " \
                  "PRIMARY KEY (`id`), " \
                  "KEY `user_id` (`user_id`) USING BTREE" \
              ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        cur.execute(sql)

# execute a list of names
db = MySQLdb.connect(host='localhost',
                user='root',
                passwd='111111',
                db='ght_msr_2014',

                local_infile=1,
                use_unicode=True,
                charset='utf8mb4',

                autocommit=False)
cur = db.cursor()

# create country_countryNameManager table
create_table(cur, "country_countryNameManager")

# read users table from ghtorrent
cur.execute("select max(user_id) from country_countryNameManager")
max_user_id = cur.fetchone()
if max_user_id[0] is None:
    max_user_id = 0
else:
    max_user_id = max_user_id[0]
print max_user_id
cur.execute("select id, city, state, country_code, location "
            "from users "
            "where id > %s", (max_user_id,))
# cur.execute("select id, city, state, country_code, location "
#             "from users "
#             "where id = 52")
users = cur.fetchall()
for user in users:
    user_id = user[0]
    city = user[1]
    state = user[2]
    country_code = user[3]
    location = user[4]

    # get country according to city
    if city == None:
        country_city = None
    else:
        country_city = cg.guess(unidecode(city).lower().strip())[0]

    # get country according to state
    if state == None:
        country_state = None
    else:
        country_state = cg.guess(unidecode(state).lower().strip())[0]

    # get country according to country_code
    if country_code == None:
        country_country_code = None
    else:
        country_country_code = cg.guess(unidecode(country_code).lower().strip())[0]

    # get country according to location
    if location == None:
        country_location = None
    else:
        country_location = cg.guess(unidecode(location).lower().strip())[0]

    result_array = [country_city, country_state, country_country_code, country_location]
    result = None
    for r in result_array:
        if r is not None and result is None:
            result = r
        elif r is not None and result is not None:
            if result != r:
                result = None
                break
        elif r is None:
            continue

    cur.execute("insert into country_countryNameManager (user_id, country) values (%s, %s)", (user_id, result))
    logging.info("user %d: %s" % (user_id, result))
    if user_id % 10000 == 0:
        db.commit()
db.commit()