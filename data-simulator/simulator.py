#!/usr/bin/env python
"""a simple data generator that sends to a Kafka broker"""
import sys
import json
import time
import random
from confluent_kafka import Producer
import socket

def generate(config, asset_0, asset_1, interval_ms, inject_error, devmode, destination):
    """generate data and send it to a Kafka broker"""

    interval_secs = interval_ms / 1000.0
    random.seed()

    if not devmode:
        if destination == "kafka":
            #prepare Kafka connection
            kafka_config = config.get("kafka", {})
            brokers = kafka_config.get("brokers", "localhost:9092")
            topic = kafka_config.get("topic", "simulator")
            kafkaconf = {'bootstrap.servers': brokers,'client.id': socket.gethostname()}
            producer = Producer(kafkaconf)
        else:    
            if destination == "file":
                file_config = config.get("file", {})
                filepath = file_config.get("filepath","output.json")
                destination_file = open(filepath, 'w+')
    

    #extract assets dimensions details
    asset_0_label = asset_0.get("label","asset_0")
    asset_0_nb_assets = asset_0.get("assets","3")
    asset_0_nb_dimensions = asset_0.get("dimensions","3")
    asset_0_dimensions_labels = asset_0.get("dimension_labels",[])
    asset_0_dimensions_types = asset_0.get("dimension_types",[])
    asset_0_dimensions_values = asset_0.get("dimension_values",[])
    asset_1_label = asset_1.get("label","asset_1")
    asset_1_nb_assets = asset_1.get("assets","3")
    asset_1_nb_dimensions = asset_1.get("dimensions","3")
    asset_1_dimensions_labels = asset_1.get("dimension_labels",[])
    asset_1_dimensions_types = asset_1.get("dimension_types",[])
    asset_1_dimensions_values = asset_1.get("dimension_values",[])
    asset_1_nb_metrics = asset_1.get("metrics",3)
    asset_1_metrics_values = asset_1.get("metrics_values")
    asset_1_metrics_labels = asset_1.get("metrics_labels")

    while True:
        data = {
            "timestamp": int(time.time()*1000000)
        }

        for a0 in range(asset_0_nb_assets):

            #GENERIC: generate asset_0 IDs
            data[asset_0_label+"_id"] = asset_0_label+"_" + str(a0)

            #GENERIC: generate asset_0 dimensions
            for key in range(asset_0_nb_dimensions):
                values = asset_0_dimensions_values.get("d_" + str(key))
                labels = asset_0_dimensions_labels.get("d_" + str(key))
                types = asset_0_dimensions_types.get("d_" + str(key))
                if types == "fixed":
                    data[labels] = values[a0]
                else:
                    if types == "high_cardinality":
                        data[labels] = labels + "_" + str(random.randint(0, values + 1))
                    else: 
                        if types == "random":
                            data[labels] = random.choice(values)

            for a1 in range(asset_1_nb_assets):
                #GENERIC: generate asset_1 IDs
                data[asset_1_label+"_id"] = asset_1_label+"_" + str(a0)+"_"+str(a1)

                #GENERIC: generate asset_1 dimensions
                for key in range(asset_1_nb_dimensions):
                    values = asset_1_dimensions_values.get("d_" + str(key))
                    labels = asset_1_dimensions_labels.get("d_" + str(key))
                    types = asset_1_dimensions_types.get("d_" + str(key))
                    if types == "fixed":
                        data[labels] = values[a1]
                    else:
                        if types == "high_cardinality":
                            data[labels] = labels + "_" + str(random.randint(0, values + 1))
                        else: 
                            if types == "random":
                                data[labels] = random.choice(values)

                #GENERIC: generate metrics
                for key in range(asset_1_nb_metrics):
                    min_val, max_val = asset_1_metrics_values.get("m_" + str(key))
                    label = asset_1_metrics_labels.get("m_" + str(key))
                    data[label] = random.randint(min_val, max_val)
              
                #Custom: Implement your abnormal behavior here ->          
                if (inject_error == 'true'):
                    data["discount"] = random.randint(20, 25)
                    data["quantity"] = random.randint(1, 99)
                # -> end of abnormal behavior

                #GENERIC: publish the data
                if devmode:
                    print(json.dumps(data, indent=4), flush=True)
                else:
                    if destination == "kafka":
                        producer.produce(topic, key=data[asset_0_label+"_id"], value=json.dumps(data))
                        producer.poll(0)
                    else:
                        if destination == "file":
                            destination_file.write(json.dumps(data) + '\n')

        time.sleep(interval_secs)

def main(config_path,inject_error):
    """main entry point, load and validate config and call generate"""
    try:
        with open(config_path) as handle:
            config = json.load(handle)

            #prepare metrics configurations
            misc_config = config.get("misc", {})
            interval_ms = misc_config.get("interval_ms", 500)
            devmode = misc_config.get("devmode", False)
            destination = misc_config.get("destination", "file")

            #prepare assets
            asset_0 = config.get("asset_0",{})
            asset_1 = config.get("asset_1",{})

            #Start simulation
            generate(config, asset_0, asset_1, interval_ms, inject_error, devmode, destination)

    except IOError as error:
        print("Error opening config file '%s'" % config_path, error)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[1],sys.argv[2])
    else:
        print("usage %s config.json plant_id yield_ratio" % sys.argv[0])