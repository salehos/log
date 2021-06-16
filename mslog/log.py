import logging
import threading
import json
from kafka import KafkaProducer
import traceback as tb
from bson.json_util import dumps, RELAXED_JSON_OPTIONS

def create_json_message(message, log_level, module_name, trace_back):
    if type(message) == str:
        try:
            message = message.replace("\'","\"")
            message = json.loads(message)
            message.update({'log_level': log_level, 'module_name': module_name,'trace_back': trace_back})
            return message
        except:
            return {
                'message': message,
                'log_level': log_level,
                'module_name': module_name,
                'trace_back': trace_back,
            }
    elif type(message) == dict:
        message = dumps(message, json_options=RELAXED_JSON_OPTIONS)
        message = json.loads(message)
        message.update({'log_level': log_level, 'module_name': module_name,'trace_back': trace_back})
        return message
        


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def send_to_kafka(kafka_servers, json_message, kafka_topic):
    producer = KafkaProducer(bootstrap_servers=kafka_servers,
                             value_serializer=json_serializer)
    producer.send(kafka_topic, json_message)


def create_kafka_thread(kafka_servers, json_message, kafka_topic):
    y = threading.Thread(target=send_to_kafka, args=(kafka_servers, json_message, kafka_topic))
    y.start()

def do_log(message, module_name, log_level, filename):
    logging.basicConfig(filename=f'/var/log/{module_name}/{filename}.log', level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d %(levelname)s : %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    if log_level == "critical":
        logging.critical(message, exc_info=True)
    elif log_level == "info":
        logging.info(message)
    elif log_level == "debug":
        logging.debug(message)
    elif log_level == "warn":
        logging.warning(message)
    elif log_level == "error":
        logging.error(message, exc_info=True)
        
class Log:
    def __init__(self, client_id, kafka_servers, module_name, kafka_logging, kafka_topic, **kwargs):
        self.client_id = client_id
        self.kafka_servers = kafka_servers
        self.module_name = module_name
        self.kafka_logging = kafka_logging
        self.kafka_topic = kafka_topic
        self.log_name = "log"
        for key, value in kwargs.items():
            if key == "log_name":
                self.log_name = value
        logging.getLogger("kafka").setLevel(logging.ERROR or logging.WARN or logging.WARNING)
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        

    def log(self, message, log_level="info", log_name=None):
        if log_name is None:
            x = threading.Thread(target=do_log, args=(message, self.module_name, log_level, self.log_name))
        else:
            x = threading.Thread(target=do_log, args=(message, self.module_name, log_level, log_name))
        x.start()
        if self.kafka_logging:
            traceback = ""
            if log_level == "error" or log_level == "critical":
                traceback = tb.format_exc()
            json_message = create_json_message(message=message, log_level=log_level, module_name=self.module_name,
                                               trace_back=traceback)
            create_kafka_thread(self.kafka_servers, json_message, self.kafka_topic)



