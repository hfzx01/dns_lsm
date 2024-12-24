import os
import json

from to_sql import create_insert


def judge(content):
    if not content:
        return 0
    elif content[0][:5] == "Error":
        return 0
    else:
        return 1


def get_range(fp):
    # 创建一个字典来存储每个文件的内容
    json_files_data = {}
    result = {}
    # 遍历目录中的所有文件
    for filename in os.listdir(fp):
        if filename.endswith(".json"):  # 只处理.json文件
            file_path = os.path.join(fp, filename)
            # 打开并读取JSON文件
            with open(file_path, 'r', encoding='utf-8') as file:
                try:
                    data = json.load(file)
                    json_files_data[filename] = data  # 存储文件路径和内容到字典
                    print(f"Successfully loaded {filename}")
                except json.JSONDecodeError as e:
                    print(f"Error reading {filename}: {e}")
    for key, value in json_files_data.items():
        for key1, value1 in value.items():
            if key1 not in result:
                result[key1] = {}
            if key.split('_')[0].split('-')[0] == '150.109.100.62':
                result[key1].update({'HK': judge(value1)})
            elif key.split('_')[0].split('-')[0] == '172.234.92.95':
                result[key1].update({'Japan': judge(value1)})
            elif key.split('_')[0].split('-')[0] == '203.6.233.160':
                result[key1].update({'GuiZhou': judge(value1)})
            elif key.split('_')[0].split('-')[0] == '106.74.15.134':
                result[key1].update({'InnerMongolia': judge(value1)})
            else:
                raise KeyError(key)
    create_insert(result)

if __name__ == '__main__':
    fp = 'service_task/task_1730016175'
    get_range(fp)