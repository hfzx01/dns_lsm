import mysql.connector
import time
import pytz as pytz
import datetime

def create_insert(result):
    values_list = []
    # 创建数据库连接
    db = mysql.connector.connect(
        host="150.109.100.62",  # MySQL服务器地址
        user="root",  # 用户名
        password="Zm.1575098153",  # 密码
        database="dns"  # 数据库名称
    )
    # 创建游标对象，用于执行SQL查询
    cursor = db.cursor()
    tz = pytz.timezone('Asia/Shanghai')  # 东八区
    table_name = datetime.datetime.fromtimestamp(int(time.time()), tz).strftime('%Y%m%d%H%M')
    # 创建一个名为"customers"的数据表
    sql1 = f"CREATE TABLE service_range_task_{table_name} (`id` int NOT NULL AUTO_INCREMENT, `dns_server` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, `HK` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,  `Japan` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,  `GuiZhou` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,  `InnerMongolia` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NULL DEFAULT NULL,`time` datetime(0) NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0),  PRIMARY KEY (`id`) USING BTREE)"
    cursor.execute(sql1)
    sql2 = f"INSERT INTO service_range_task_{table_name} (dns_server, HK, Japan, GuiZhou, InnerMongolia) VALUES (%s, %s, %s, %s, %s)"
    for key, value in result.items():
        values = (key, value['HK'], value['Japan'], value['GuiZhou'], value['InnerMongolia'])
        values_list.append(values)
    cursor.executemany(sql2, values_list)
    # 提交更改到数据库
    db.commit()
    cursor.close()  # 关闭游标
    db.close()  # 关闭数据库连接