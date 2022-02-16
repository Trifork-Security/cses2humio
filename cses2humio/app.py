import argparse
import json
import logging
import os
import signal
import sys
import threading
from datetime import datetime, timedelta
from os.path import exists
from urllib.parse import urljoin, urlparse

import requests
from falconpy import EventStreams

from cses2humio import __version__

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

log = logging.getLogger(__name__)

lock = threading.Lock()
exit_event = threading.Event()


def write_offset(file, partition, offset):
    with open(file, "r+") as f:
        data = json.load(f)  # load file
        data[str(partition)] = offset  # set offset
        f.seek(0)  # reset file point
        f.write(json.dumps(data, sort_keys=True))  # write data
        f.truncate()  # remove rest


def get_offset(file, partition):
    with open(file, "r") as f:
        data = json.load(f)

    return data.get(str(partition), 0)


def create_offset_file(file):
    log.debug(f"Creating offset file : {file}")
    with open(file, "w+") as f:
        data = {}
        f.write(json.dumps(data))


def partition_stream_thread(args, falcon, humio, stream):
    # Get stream information
    stream_url = stream["dataFeedURL"]
    stream_token = stream["sessionToken"]["token"]
    refresh_interval = int(stream["refreshActiveSessionInterval"])

    # Extract partition from stream URL
    partition = int(urlparse(stream_url).path.split("/")[-1])

    # Set thread name for intuitive logging
    threading.current_thread().name = f"partition-{partition}"

    stream_headers = {
        "Authorization": f"Token {stream_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "User-Agent": args.user_agent,
    }

    params = {
        "offset": int(get_offset(args.offset_file, partition)),
    }

    events = list()
    last_refresh = datetime.utcnow()
    last_flush = datetime.utcnow()

    log.info(f"I'm ready! Starting the stream at {params['offset']} for partition {partition}")
    with requests.get(
        url=stream_url, headers=stream_headers, params=params, stream=True
    ) as r:
        for line in r.iter_lines():
            try:
                if line:
                    decoded_line = line.decode("utf-8")
                    log.debug(decoded_line)

                    if args.enrich:
                        # Load the json event
                        json_event = json.loads(decoded_line)

                        # Create the Humio event with timestamp and readable rawstring
                        event = {
                            "timestamp": json_event["metadata"]["eventCreationTime"],
                            "rawstring": decoded_line,
                        }

                        # Parse AuditKeyValues for better use in Humio
                        audit_kv = json_event["event"].get("AuditKeyValues")
                        if audit_kv:
                            for kv in audit_kv:
                                json_event["event"][kv["Key"]] = kv["ValueString"]
                            json_event["event"].pop("AuditKeyValues", None)

                        event["attributes"] = json_event
                        events.append(event)
                    else:
                        # Append to event list
                        events.append(decoded_line)

                # If flush_wait_time or bulk_max_size exceeded and have events in queue (love black format!)
                if (
                    datetime.utcnow()
                    >= (last_flush + timedelta(seconds=args.flush_wait_time))
                    or (len(events) >= args.bulk_max_size)
                ) and len(events) != 0:
                    if args.enrich:
                        offset = json_event["metadata"]["offset"] + 1
                    else:
                        # We only parse the last event to get the current offset
                        offset = json.loads(decoded_line)["metadata"]["offset"] + 1

                    ingest = requests.post(
                        url=humio["url"],
                        headers=humio["header"],
                        json=[{humio["event_keyword"]: events}],
                    )
                    if ingest.status_code != 200:
                        raise Exception(
                            "Error shipping logs to Humio : {}".format(ingest.content)
                        )

                    # Obtain thread lock and write offset
                    with lock:
                        write_offset(args.offset_file, partition, offset)

                    # Log current status
                    log.info(
                        f"Shipped {len(events)} events to Humio. "
                        f"We're now at offset {offset} for partition {partition}."
                    )

                    # Clear event queue and update last flush time
                    events.clear()
                    last_flush = datetime.utcnow()

                # Check if we have exceeded 90% of refresh time, then refresh.
                if (
                    last_refresh + timedelta(minutes=(refresh_interval * 0.9))
                ) <= datetime.utcnow():
                    refresh = falcon.refresh_active_stream(
                        app_id=args.app_id,
                        partition=partition,
                        user_agent=args.user_agent,
                    )

                    if refresh["status_code"] != 200:
                        raise Exception(f"Failed to refresh stream : {refresh}")

                    log.debug(refresh)
                    log.info("Refreshed active stream.")
                    last_refresh = datetime.utcnow()

                if exit_event.is_set():
                    log.info("Going to exit...")
                    break

            except Exception as e:
                exit_event.set()
                log.exception("Got unexpected error in thread, ending all threads.")
                break


def signal_handler(signum, frame):
    log.info(f"Got {signal.Signals(signum).name}, exiting...")
    exit_event.set()


def app_run(args):
    pp_args(args)

    if args.verbose:
        log.info("Going verbose...")
        log.setLevel(logging.DEBUG)

    if not exists(args.offset_file):
        create_offset_file(args.offset_file)

    log.info("Obtaining OAuth2 token to Falcon")
    falcon = EventStreams(
        base_url=args.falcon_url,
        client_id=args.falcon_api_id,
        client_secret=args.falcon_api_secret,
        user_agent=args.user_agent,
    )

    if not falcon.token:
        log.error(f"Failed to obtain OAuth2 token : {falcon.token_fail_reason}")
        sys.exit(1)

    log.info("Getting available event streams")
    streams = falcon.list_available_streams(app_id=args.app_id, format="json")
    log.debug(json.dumps(streams))

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if args.enrich:
        event_keyword = "events"
        humio_endpoint = "/api/v1/ingest/humio-structured"
    else:
        event_keyword = "messages"
        humio_endpoint = "/api/v1/ingest/humio-unstructured"

    humio = {
        "url": urljoin(args.humio_url, humio_endpoint),
        "event_keyword": event_keyword,
        "header": {
            "Authorization": f"Bearer {args.humio_token}",
            "Content-Type": "application/json",
            "User-Agent": args.user_agent,
        },
    }

    log.info("Starting thread for each stream")
    threads = []
    for stream in streams["body"]["resources"]:
        t = threading.Thread(
            target=partition_stream_thread,
            args=(
                args,
                falcon,
                humio,
                stream,
            ),
            daemon=True,
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


def pp_args(args):
    print("Starting with the following arguments:\n")
    for arg in vars(args):
        if arg not in ("falcon_api_secret", "humio_token"):
            val = getattr(args, arg)
        else:
            if getattr(args, arg):
                val = "[HIDDEN]"
            else:
                val = "NOT SET"

        print("\t{0:<16} \t\t=>\t {1}".format(arg, val))
    print("")


def cli():
    parser = argparse.ArgumentParser(
        description="CrowdStrike Falcon Event Stream to Humio"
    )

    # Add groups
    general = parser.add_argument_group("General")
    falcon = parser.add_argument_group("Falcon")
    humio = parser.add_argument_group("Humio")
    advanced = parser.add_argument_group("Advanced")

    general.add_argument(
        "--offset-file",
        default="offset.db",
        type=str,
        action="store",
        help="Location including filename for where to store offsets, default is current directory as offset.db",
    )

    general.add_argument(
        "--enrich",
        action="store_true",
        help="Will parse some fields as they're hard to parse in Humio."
        "Note this might be more resources intensive but spare Humio of parsing. "
        "Default is off",
    )

    general.add_argument(
        "-v", "--verbose", help="Increase output verbosity", action="store_true"
    )

    falcon.add_argument(
        "--falcon-url",
        default="https://api.crowdstrike.com",
        type=str,
        action="store",
        help="Falcon API URL, note this is for the API given when you create the API key. Defaults to US-1 API url",
    )

    falcon.add_argument(
        "--falcon-api-id",
        type=str,
        action="store",
        help="Falcon API ID to use for OAuth2",
    )

    falcon.add_argument(
        "--falcon-api-secret",
        type=str,
        action="store",
        help="Falcon API Secret to use for OAuth2",
    )

    humio.add_argument(
        "--humio-url",
        default="https://cloud.humio.com",
        type=str,
        action="store",
        help="Humio URL for the cluster going to ingest data. Default to https://cloud.humio.com",
    )

    humio.add_argument(
        "--humio-token",
        type=str,
        action="store",
        help="Ingest token to use for ingesting data. Remember to assign the correct parser depending on parsing",
    )

    advanced.add_argument(
        "--app-id",
        default="cses2humio",
        type=str,
        action="store",
        help="App ID to use for consuming events",
    )

    advanced.add_argument(
        "--user-agent",
        default=f"cses2humio/{__version__}",
        type=str,
        action="store",
        help="User agent used to connect to services",
    )

    advanced.add_argument(
        "--bulk-max-size",
        default=200,
        type=str,
        action="store",
        help="Maximum number of events to send in bulk",
    )

    advanced.add_argument(
        "--flush-wait-time",
        default=10,
        type=str,
        action="store",
        help="Maximum time to wait if bulk max size isn't reached",
    )

    args = parser.parse_args()

    missing_args = []
    for arg in vars(args):
        env = os.environ.get(arg.upper())
        if env:
            if arg in ("bulk_max_size", "bulk_max_size"):
                env = int(env)
            elif arg in ("verbose", "enrich"):
                env = env.lower() in ("true", "1", "t")

            setattr(args, arg, env)

        if getattr(args, arg) is None:
            missing_args.append(arg.upper())

    if missing_args:
        log.error(f"Please set missing variables: {', '.join(missing_args)}")
        sys.exit(1)

    app_run(args)


if __name__ == "__main__":
    cli()
