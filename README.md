# log
python logger library

how to install(for the begginers):
pip install -e git://github.com/salehos/log.git@[last_commit]#egg=mslog
 ```console
 $ pip install -e git://github.com/salehos/log.git@6999ae08d158be3c8a5e93c3732363d282bce07c#egg=mslog
```


for Bson type use this version

 ```console
$ pip install -e git://github.com/salehos/log.git@173a894736338891c171a08ea9ecd40e33b45f42#egg=mslog
```

how to use:

1- import mslog to your project
 ```console
$ import mslog
```

2- create an instance from Log class 

 ```console
$ log_module = mslog.Log(client_id=config.logging_client_id, kafka_servers=config.kafka_servers,
                           module_name=config.module_name, kafka_logging=config
                           .kafka_logging, local_logging=config.local_logging , kafka_topic=config.kafka_log_topic, log_name = config.log_name)
```

3- use .log method to log like this:(for info log)

 ```console
$ log_module.log(message=message)
```

or for error or debug logs:

 ```console
$ log_module.log(message=message, log_level = "error")
```
