#!/usr/bin/env python
"""a simple data generator that sends to a Kafka broker"""
import sys
import json
import time
import random
from confluent_kafka import Producer
import socket

def generate(producer, topic, asset_0, asset_1, nb_metrics, metrics_values, metrics_labels, interval_ms, inject_error, devmode):
    """generate data and send it to a Kafka broker"""

    interval_secs = interval_ms / 1000.0
    random.seed()
    iteration = 0

    #extract assets dimensions details
    asset_0_label = asset_0.get("label","asset_0")
    asset_0_nb_assets = asset_0.get("assets","3")
    asset_0_nb_dimensions = asset_0.get("dimensions","3")
    asset_0_dimensions_values = asset_0.get("dimension_values",[])
    asset_0_dimensions_labels = asset_0.get("dimension_labels",[])
    asset_1_label = asset_1.get("label","asset_1")
    asset_1_nb_assets = asset_1.get("assets","3")
    asset_1_nb_dimensions = asset_1.get("dimensions","3")
    asset_1_dimensions_values = asset_1.get("dimension_values",[])
    asset_1_dimensions_labels = asset_1.get("dimension_labels",[])


    while True:
        iteration = iteration+1

        data = {
            "sensor_ts": int(time.time()*1000000)
        }

        for a0 in range(asset_0_nb_assets):

            #GENERIC: generate asset_0 IDs
            data[asset_0_label+"_id"] = asset_0_label+"_" + str(a0)

            #GENERIC: generate asset_0 dimensions
            for key in range(asset_0_nb_dimensions):
                values = asset_0_dimensions_values.get("d_" + str(key))
                label = asset_0_dimensions_labels.get("d_" + str(key))
                data[label] = values[a0]

            for a1 in range(asset_1_nb_assets):
                #GENERIC: generate asset_1 IDs
                data[asset_1_label+"_id"] = asset_1_label+"_" + str(a0)+"_"+str(a1)

                #GENERIC: generate asset_1 dimensions
                for key in range(asset_1_nb_dimensions):
                    values = asset_1_dimensions_values.get("d_" + str(key))
                    label = asset_1_dimensions_labels.get("d_" + str(key))
                    data[label] = values[a1]

                #GENERIC: generate metrics
                for key in range(nb_metrics):
                    min_val, max_val = metrics_values.get("m_" + str(key))
                    label = metrics_labels.get("m_" + str(key))
                    data[label] = random.randint(min_val, max_val)
              
                #Custom: Implement your abnormal behavior here ->
                if (iteration == 10):
                    data["rejected"] = random.randint(0, 3)
                if (a0 == 0 and (a1 == 0 or a1 == 4)):
                    # that's the case of the plant that solved the issue
                    data["machine_configuration"] = "multi_layer_custom"            
                if (inject_error == 'true'):
                    if (a0 == 4 and (a1 == 0 or a1 == 4)):
                        data["rejected"] = random.randint(1, 2)
                        data["temperature"] = random.randint(60, 65)
                        data["vibration"] = random.randint(120, 130)
                        data["materials"] = "silicon_multi_layers"
                    #else:
                        #data["machine_configuration"] = "multi_layer_custom" 
                # -> end of abnormal behavior

                #GENERIC: publish the data
                payload = json.dumps(data)
                if devmode:
                    print(payload)
                else:
                    producer.produce(topic, key=data["machine_id"], value=payload)
                    producer.poll(0)

        time.sleep(interval_secs)
        if (iteration == 10):
            iteration = 0

        


def main(config_path,inject_error):
    """main entry point, load and validate config and call generate"""
    try:
        with open(config_path) as handle:
            config = json.load(handle)

            #prepare metrics configurations
            misc_config = config.get("misc", {})
            interval_ms = misc_config.get("interval_ms", 500)
            devmode = misc_config.get("devmode", False)

            #prepare assets
            asset_0 = config.get("asset_0",{})
            asset_1 = config.get("asset_1",{})
            

            #prepare metrics configurations
            nb_metrics = misc_config.get("metrics",3)
            metrics_values = config.get("metrics_values")
            metrics_labels = config.get("metrics_labels")



            plants = config.get("plants")
            
            providers = config.get("providers")
            materials = config.get("materials")
            machine_providers = config.get("machine_providers")

            kafka_config = config.get("kafka", {})
            brokers = kafka_config.get("brokers", "localhost:9092")
            topic = kafka_config.get("topic", "iot")

            kafkaconf = {'bootstrap.servers': brokers,'client.id': socket.gethostname()}
            producer = Producer(kafkaconf)

            generate(producer, topic, asset_0, asset_1, nb_metrics, metrics_values, metrics_labels, interval_ms, inject_error, devmode)

    except IOError as error:
        print("Error opening config file '%s'" % config_path, error)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[1],sys.argv[2])
    else:
        print("usage %s config.json plant_id yield_ratio" % sys.argv[0])