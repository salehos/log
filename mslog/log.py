import logging
import threading
import json
import confluent_kafka
import traceback as tb
from requests.exceptions import ConnectTimeout

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
        message.update({'log_level': log_level, 'module_name': module_name,'trace_back': trace_back})
        return message
    else:
    	return {
                'message': str(message),
                'log_level': log_level,
                'module_name': module_name,
                'trace_back': trace_back,
            }


        
        
        


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(48)
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))

def send_to_kafka(kafka_servers, json_message, kafka_topic):
    p = confluent_kafka.Producer({'bootstrap.servers': kafka_servers})
    p.poll(0)
    p.produce(kafka_topic,json.dumps(json_message),callback=delivery_report)
    try:
        re = p.flush(timeout=10)
        if re > 0:
            raise ConnectTimeout
    except:
        raise ConnectTimeout
    


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
    def __init__(self, client_id, kafka_servers, module_name, kafka_logging, local_logging, kafka_topic, **kwargs):
        self.client_id = client_id
        self.kafka_servers = kafka_servers
        self.module_name = module_name
        self.kafka_logging = kafka_logging
        self.kafka_topic = kafka_topic
        self.local_logging = local_logging
        self.log_name = "log"
        for key, value in kwargs.items():
            if key == "log_name":
                self.log_name = value
        logging.getLogger("kafka").setLevel(logging.ERROR or logging.WARN or logging.WARNING)
        logging.getLogger("requests").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        

    def log(self, message, log_level="info", log_name=None):
        try :
            if self.local_logging:
                if log_name is None:    
                    x = threading.Thread(target=do_log, args=(message, self.module_name, log_level, self.log_name))
                else:
                    x = threading.Thread(target=do_log, args=(message, self.module_name, log_level, log_name))
                x.start()
            else:
                pass
        except Exception as e:
            raise e

        try:
            if self.kafka_logging:
                traceback = ""
                if log_level == "error" or log_level == "critical":
                    traceback = tb.format_exc()
                json_message = create_json_message(message=message, log_level=log_level, module_name=self.module_name,
                                                   trace_back=traceback)
                create_kafka_thread(self.kafka_servers, json_message, self.kafka_topic)
        except Exception as e:
            raise e



