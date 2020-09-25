## Imply Data Simulator

There's no good demo without good data. You can spend days searching for a perfect dataset for your use case, or you can build such a dataset. Building the dataset let you be creative and create a without limits. You can then shape the data stream in a way that support your story. It enables you to put the right characteristics in your stream to show how much the Imply platform is great (slice & dice, alerting, explain, etc.). However, building such a complete data stream is time consuming.

## The goal 

The goal of this tool is to create a very lightweight and simple framework to easily generate data streams for demos.  The framework is written in Python and uses a configuration JSON configuration file. The ultimate objective is to be able generate any new data streams just by changing the configuration file.

The simulator has the following feature:
1. Support hierarchical assets (ex. Company -> Store -> Point of sale, Organization -> Business Unit -> Division -> Trader)
2. Generate events in real time with a configurable frequency
3. Publish data to a Kafka topic
4. Generate metrics with a random distribution
5. Generate dimensions with low or high cardinalities
6. Support normal and abnormal scenarios (ex. sales numbers go down, error rate goes up, shipping cost skyrockets)
7. Support a devmode to display the simulated data in the console for verifying purpose
8. Has a set of helpers to control the simulation (start/stop/restart/status/switch)
9. Comes with a set of pre-built demo examples


This project is a WIP and can be extended to support new use cases. The following extensions are planned. Please log an issue if you have an idea to make the project better. Even better, submit a PR for your idea :)
1. Support publishing data to Kinesis
2. Support writing data to a file
3. Support other distribution than random
4. Integrate data simulator API such as Mockaroo
5. Create new demo examples


## Prerequisites

You need to have Python3 and Git installed in the machine where you would like to run the simulator. Please note that the script uses several libraries that you need to install as well (see preparing the simulator). For publishing the data to Kafka you need to have a Kafka cluster that's accessible from the machine where you are running the simulator. 

## Preparing the simulator

To prepare the prerequisites for the simulator on an EC2 RHEL instance, run the following instructions:

```shell
sudo yum update
sudo yum -y install git python3
sudo pip3 install confluent_kafka
git clone https://github.com/ahadjidj/imply-toolbox.git
cd ./imply-toolbox/data-simulator/
```

## Running the simulator

To check that everything is configured correctly, you can run the default template. It generates retail events in devmode and logs them in /tmp/simulator.log

```shell
./simulator.sh start
```

To check that everything is working as expected, you can run ``tail -f /tmp/simulator.log`` and verify that new events are produced every 10 seconds. Also, run ``./simulator.sh status`` and make sure that the output looks like:

```shell
==== Status

Pid file: 54620 [/tmp/simulator.pid]

ec2-user   54620       1  1 13:34 pts/0    00:00:00 python3 /home/ec2-user/imply-toolbox/data-simulator/simulator.py /home/ec2-user/imply-toolbox/data-simulator/simulator.config false

Running in normal mode
```

Note the "running in the normal mode". To move to the abnormal mode, you can run ``./simulator.sh switch``. When you do so, you should see the events being generated with higher "quantity" and "discount". This is to simulate a higher discount and its impact on sales. Again, run ``./simulator.sh status`` and make sure that the output says that you are running in abnormal mode:

```shell
==== Status

Pid file: 54637 [/tmp/simulator.pid]

ec2-user   54637       1  1 13:37 pts/0    00:00:00 python3 /home/ec2-user/imply-toolbox/data-simulator/simulator.py /home/ec2-user/imply-toolbox/data-simulator/simulator.config true

Running in abnormal mode
```

You can now stop the simulator with ``./simulator.sh stop``.

## Using a pre-built example

The example folder comes with industry focused example that you use and extend. To run the Industry 4.0 example, you can simply copy the files into the main directory and run the simulator like previously.

```shell
cp ./examples/industry4-0.config ./simulator.config
cp ./examples/industry4-0-simulator.py ./simulator.py
```

You should see IIoT data generated every second.

## Build your own example

TODO
