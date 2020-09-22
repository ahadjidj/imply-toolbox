#!/usr/bin/env python
"""a simple data generator that sends to a Kafka broker"""
import sys
import json
import time
import random
from confluent_kafka import Producer
import socket

def generate(producer, topic, nb_plants, nb_machines, plants, machines, labels, providers, materials, machine_providers, interval_ms, inject_error, devmode):
    """generate data and send it to a Kafka broker"""

    interval_secs = interval_ms / 1000.0

    random.seed()
    h = 0
    while True:
        h = h+1

        data = {
            "sensor_ts": int(time.time()*1000000)
        }

        for p in range(nb_plants):
            for m in range(nb_machines):
                data["plant_id"] = "plant_" + str(p)
                data["machine_id"] = "machine_" + str(p) + "_" + str(m)
                data["configuration"] = "vendor_default"
                data["accepted"] = random.randint(5, 10)
                data["rejected"] = 0
                data["provider"] = providers.get("plant_"+ str(p))
                data["materials"] = materials.get("plant_"+ str(p))
                data["machine_providers"] = machine_providers.get("machine_"+ str(m))
                data["geo_country"] = plants.get("plant_"+ str(p))
                data["city"] = plants.get("plant_"+ str(p)+"_city") 
                for key in range(10):
                    min_val, max_val = machines.get("sensor_" + str(key))
                    label = labels.get("sensor_" + str(key))
                    data[label] = random.randint(min_val, max_val)

                # injecting normal error every 10 events to stay within 3%
                if (h == 10):
                    data["rejected"] = random.randint(0, 3)

                
                #handling the quality issue combination
                if (m == 0 or m == 4):
                    if (p == 0): # that's the case of the plant that solved the issue
                        data["configuration"] = "multi_layer_custom"            
                    else:
                        if (p == 4):
                            if (inject_error == 'true'):
                                data["rejected"] = random.randint(1, 2)
                                data["temperature"] = random.randint(60, 65)
                                data["vibration"] = random.randint(120, 130)
                                data["materials"] = "silicon_multi_layers"
                            else:
                                data["configuration"] = "multi_layer_custom" 

                payload = json.dumps(data)

                if devmode:
                    print(payload)
                else:
                    producer.produce(topic, key=data["machine_id"], value=payload)
                    producer.poll(0)

        time.sleep(interval_secs)
        if (h == 10):
            h = 0

        


def main(config_path,inject_error):
    """main entry point, load and validate config and call generate"""
    try:
        with open(config_path) as handle:
            config = json.load(handle)

            misc_config = config.get("misc", {})
            interval_ms = misc_config.get("interval_ms", 500)
            devmode = misc_config.get("devmode", False)
            nb_plants = misc_config.get("plants",3)
            nb_machines = misc_config.get("machines",3)

            plants = config.get("plants")
            machines = config.get("machines")
            labels = config.get("labels")
            providers = config.get("providers")
            materials = config.get("materials")
            machine_providers = config.get("machine_providers")

            kafka_config = config.get("kafka", {})
            brokers = kafka_config.get("brokers", "localhost:9092")
            topic = kafka_config.get("topic", "iot")

            kafkaconf = {'bootstrap.servers': brokers,'client.id': socket.gethostname()}
            producer = Producer(kafkaconf)

            generate(producer, topic, nb_plants, nb_machines, plants, machines, labels, providers, materials, machine_providers, interval_ms, inject_error, devmode)

    except IOError as error:
        print("Error opening config file '%s'" % config_path, error)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[1],sys.argv[2])
    else:
        print("usage %s config.json plant_id yield_ratio" % sys.argv[0])