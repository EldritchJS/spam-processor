import argparse
import logging

import os
import threading

import kafka
import requests

exit_event = threading.Event()

from urllib.parse import urlencode
import json


DEFAULT_BASE_URL = "http://pipeline:8080/%s"


def score_text(text, url=None):
    url = url or (DEFAULT_BASE_URL % "predict")
    if type(text) == str:
        text = [text]
    payload = urlencode({"json_args": json.dumps(text)})
    headers = {"content-type": "application/x-www-form-urlencoded"}
    response = requests.request("POST", url, data=payload, headers=headers)
    return response.text


def consumer(args):
    attempts = 1
    ready = False
    kc = None

    while not ready:
        logging.info("starting kafka consumer (attempt %d)" % attempts)
        attempts += 1
        try:
            kc = kafka.KafkaConsumer(args.topic, bootstrap_servers=args.brokers)
            ready = True
        finally:
            pass

    msg_count = 0
    for msg in kc:
        if exit_event.is_set():
            logging.info("exiting upon request")
            break
        try:
            msg_count = msg_count + 1
            if msg_count % 100 == 0:
                logging.info("scoring message %d" % msg_count)
            score_text(json.loads(str(msg.value, "utf-8"))["text"])
        except Exception as e:
            logging.error(e.message)
    logging.info("exiting kafka consumer")


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, "") != "" else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg("KAFKA_BROKERS", args.brokers)
    args.topic = get_arg("KAFKA_TOPIC", args.topic)
    return args


def main(args):
    exit_event.clear()
    # setup consumer thread
    cons = threading.Thread(group=None, target=consumer, args=(args,))
    cons.start()

    # exit_event.set()
    cons.join()
    logging.info("exiting spam-processor")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("starting spam-processor")
    parser = argparse.ArgumentParser(description="listen for some stuff on kafka")
    parser.add_argument(
        "--brokers",
        help="The bootstrap servers, env variable KAFKA_BROKERS",
        default="kafka.kafka.svc:9092",
    )
    parser.add_argument(
        "--topic",
        help="Topic to read from, env variable KAFKA_TOPIC",
        default="social-firehose",
    )
    args = parse_args(parser)
    main(args)
    logging.info("exiting")
