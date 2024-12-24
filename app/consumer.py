import asyncio
import aiodns
import uvloop
import multiprocessing
from multiprocessing import Process, Queue
import math
import time
import json
import lzma
import pika
import warnings
import tqdm
import get_id
import resource
import pytz
import datetime
# 忽略DeprecationWarning警告
warnings.filterwarnings('ignore', category=DeprecationWarning)

# 使用 uvloop 提升 asyncio 的性能
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

new_soft_limit = 1048576

(rlimit_nofile_soft, rlimit_nofile_hard) = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft_limit, rlimit_nofile_hard))


def get_current_timestamp():
    tz = pytz.timezone('Asia/Shanghai')  # 东八区
    return datetime.datetime.fromtimestamp(int(time.time()), tz).strftime('%Y-%m-%d-%H-%M-%S')


def get_resolve_type(resolve_type="A"):
    """获取解析类型"""
    return resolve_type


def list_split(list_temp, n):
    """分割列表，每份基本平均n个元素"""
    for i in range(0, len(list_temp), n):
        yield list_temp[i:i + n]


async def resolve_domain(domain, dns_server, record_type):
    resolver = aiodns.DNSResolver(nameservers=[dns_server], timeout=2, tries=2)
    try:
        result = await resolver.query(domain, record_type)
        # 处理不同类型的结果
        if isinstance(result, list):  # 对于 'A'、'AAAA' 等类型
            return {domain: {dns_server: [str(r.host) for r in result]}}
        elif hasattr(result, 'cname'):  # 对于 'CNAME' 查询返回单一对象的情况
            return {domain: {dns_server: [str(result.cname)]}}
        else:
            return {domain: {dns_server: ["Unexpected result format"]}}
    except aiodns.error.DNSError as e:
        return {domain: {dns_server: [f"Error: {str(e)}"]}}


async def resolve_domains(domains, dns_servers, record_type, semaphore):
    async with semaphore:
        tasks = []
        for domain in domains:
            for dns_server in dns_servers:
                task = resolve_domain(domain, dns_server, record_type)
                tasks.append(task)

        # Gather results with tqdm progress bar
        results = []
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing DNS Queries"):
            result = await f
            results.append(result)
    return results


def run_async_tasks(task_id, domains, dns_servers, resolve_type, coroutine_num):
    semaphore = asyncio.Semaphore(coroutine_num)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    results = loop.run_until_complete(resolve_domains(domains, dns_servers, resolve_type, semaphore))
    loop.close()
    return results


class QueryProcess(Process):
    def __init__(self, task_id, process_name, q, domains, dns_list, resolve_type, coroutine_num):
        super().__init__()
        self.process_name = process_name
        self.q = q
        self.domains = domains
        self.dns_list = dns_list
        self.resolve_type = resolve_type
        self.coroutine_num = coroutine_num
        self.task_id = task_id

    def run(self):
        result = run_async_tasks(self.task_id, self.domains, self.dns_list, self.resolve_type, self.coroutine_num)
        # Restructure the results to the desired format
        # structured_results = {}
        # for domain in self.domains:
        #     structured_results[domain] = {}
        #     for dns_server in self.dns_list:
        #         server_result = next((r[dns_server] for r in result if dns_server in r), ["No Result"])
        #         structured_results[domain][dns_server] = server_result
        self.q.put(result)


def get_process_num(process_times):
    """根据CPU数量，获取进程数量"""
    process_num = multiprocessing.cpu_count()
    return int(math.ceil(process_num * process_times))


def allocating_task(task_id, domains, dns_servers, resolve_type, process_times=0.5, coroutine_num=50, batch_size=20000):
    """
    多进程+协程分配任务，按批次处理域名。
    每批处理batch_size数量的域名，处理完一批后再处理下一批。
    """
    manager = multiprocessing.Manager()
    q = manager.Queue()
    process_num = get_process_num(process_times)
    print('process_num:', process_num)

    # 计算总批次数量
    num_batches = math.ceil(len(domains) / batch_size)
    all_results = {}

    # 创建tqdm进度条，跟踪批次进度
    progress_bar = tqdm.tqdm(total=num_batches, desc=f"Task {task_id} Batch Progress", unit="batch")

    for batch_num in range(num_batches):
        print(f"Processing batch {batch_num + 1}/{num_batches}")
        # 获取当前批次的域名列表
        batch_domains = domains[batch_num * batch_size: (batch_num + 1) * batch_size]

        # 将当前批次的域名列表分配给多个进程
        avg_list = list_split(batch_domains, math.ceil(len(batch_domains) / process_num))
        try:
            p_list = [
                QueryProcess(task_id, f'Process_{i}', q, each_list, dns_servers, resolve_type, coroutine_num)
                for i, each_list in enumerate(avg_list)
            ]
            for p in p_list:
                p.start()
            for p in p_list:
                p.join()
        except Exception as e:
            print(f"Error processing batch {batch_num + 1}: {e}")

        # 收集当前批次的结果
        batch_results = {}
        while not q.empty():
            tmp = q.get()
            for result in tmp:
                for key, value in result.items():
                    if key not in batch_results:
                        batch_results[key] = {}
                    batch_results[key].update(value)

        # 合并当前批次结果到总结果中
        all_results.update(batch_results)

        # 更新进度条
        progress_bar.update(1)

    # 关闭进度条
    progress_bar.close()

    return all_results


class Consumer:
    def __init__(self):
        self.queue_name = get_id.get_unique_identifier()
        self.start_time = None
        self.end_time = None

    def on_request(self, ch, method, properties, body):
        self.start_time = get_current_timestamp()
        try:
            message_data = json.loads(lzma.decompress(body).decode())
            task_id = message_data["task_id"]
            domains = message_data["domains"]
            dns_servers = message_data["dns_servers"]
            resolve_type = get_resolve_type(message_data["resolve_type"])
            results = allocating_task(task_id, domains, dns_servers, resolve_type)
        except json.JSONDecodeError as e:
            results = None
            print(f"Error decoding JSON: {e}")

        self.end_time = get_current_timestamp()
        head = f"{self.queue_name}_{self.start_time}_{self.end_time}"
        response = {"head": head, "body": results}
        compress_info = lzma.compress(json.dumps(response).encode())

        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=compress_info
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def receive_message(self):
        credentials = pika.PlainCredentials('admin', 'Liuling123!')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='8.210.155.15', port=5672, virtual_host='producer', heartbeat=0,
                                      credentials=credentials))
        channel = connection.channel()
        channel.exchange_declare(exchange='test', durable=True, exchange_type='fanout')
        result = channel.queue_declare(queue=self.queue_name, exclusive=True)
        channel.queue_bind(exchange='test', queue=result.method.queue)
        channel.basic_consume(queue=result.method.queue, on_message_callback=self.on_request, auto_ack=False)
        channel.start_consuming()


if __name__ == '__main__':
    consumer = Consumer()
    consumer.receive_message()
