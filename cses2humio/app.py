import argparse
import json
import logging
import os
import signal
import socket
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


def random_app_id(app_id):
    from random import randint

    rand_len = 31 - len(app_id)
    rand = "".join(str(randint(0, 9)) for _ in range(rand_len))
    return f"{app_id}-{rand}"


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


def retrieve_partition_from_url(url):
    return int(urlparse(url).path.split("/")[-1])


def stream_thread(args, falcon, humio, stream):
    partition = stream["partition"]
    refresh_interval_delta = stream["refresh_delta"]

    headers = {
        "Authorization": f"Token {stream['token']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "User-Agent": args.user_agent,
    }

    params = {
        "offset": int(get_offset(args.offset_file, partition)),
    }

    last_flush = datetime.utcnow()
    events = list()

    next_refresh = datetime.utcnow() + refresh_interval_delta
    log.info(
        f"Next refresh of stream at {next_refresh}Z. "
        f"We got refresh interval of {stream['refresh_interval']}. "
        f"Setting it to 85% before, {refresh_interval_delta}"
    )

    while not exit_event.is_set():
        try:
            with requests.get(
                url=stream["url"],
                headers=headers,
                params=params,
                stream=True,
                timeout=args.stream_timeout,
            ) as r:
                for line in r.iter_lines():
                    if line:
                        decoded_line = line.decode("utf-8")
                        log.debug(decoded_line)

                        if args.enrich:
                            # Load the json event
                            try:
                                json_event = json.loads(decoded_line)
                            except json.decoder.JSONDecodeError:
                                if args.verbose:
                                    log.error(
                                        f"Unable to parse json for message : {decoded_line}"
                                    )
                                continue

                            # Create the Humio event with timestamp and readable rawstring
                            event = {
                                "timestamp": json_event["metadata"][
                                    "eventCreationTime"
                                ],
                                "rawstring": decoded_line,
                            }

                            # Parse AuditKeyValues for better use in Humio
                            audit_kv = json_event["event"].get("AuditKeyValues")
                            if audit_kv:
                                for kv in audit_kv:
                                    json_event["event"][kv["Key"]] = kv["ValueString"]
                                json_event["event"].pop("AuditKeyValues", None)

                            event["attributes"] = {**humio["metadata"], **json_event}
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
                            log.debug(f"Failed to ingest events : {ingest}")
                            log.error(
                                f"Failed to ingest events. "
                                f"Trying again in next flush or bulk {args.flush_wait_time}"
                            )
                        else:
                            # Obtain thread lock and write offset
                            with lock:
                                write_offset(args.offset_file, partition, offset)

                            # Log current status
                            log.info(
                                f"Shipped {len(events)} events to Humio. "
                                f"We're now at offset {offset} for partition {partition}."
                            )

                            events.clear()  # Clear event queue and update last flush time

                        last_flush = datetime.utcnow()

                    # Check if we have exceeded 90% of refresh time, then refresh.
                    if next_refresh <= datetime.utcnow():
                        refresh = falcon.refresh_active_stream(
                            app_id=args.app_id,
                            partition=partition,
                            user_agent=args.user_agent,
                        )

                        if refresh["status_code"] != 200:
                            log.debug(f"Failed to refresh stream : {refresh}")
                            next_refresh = datetime.utcnow() + timedelta(seconds=30)
                            log.error(
                                f"Failed to refresh stream, trying in 30 seconds at {next_refresh}"
                            )
                        else:
                            log.debug(f"Refreshed the stream : {refresh}")
                            next_refresh = datetime.utcnow() + refresh_interval_delta
                            log.info(
                                f"Refreshed active stream. Next refresh at {next_refresh}"
                            )

                    if exit_event.is_set():
                        r.close()
                        log.info("Going to exit...")
                        break

        except (requests.exceptions.ConnectionError, socket.timeout):
            if args.verbose and args.exceptions:
                log.exception("ConnectionError")

            log.error(f"Stream for partition {stream['partition']} timed out.")

            r.close()
            sys.exit(1)

    log.debug("Stream exited")


# noinspection PyUnusedLocal
def signal_handler(signum, frame):
    log.info(f"Got {signal.Signals(signum).name}, exiting...")
    exit_event.set()


def start_thread(args, falcon, humio, stream):
    t = threading.Thread(
        target=stream_thread,
        args=(
            args,
            falcon,
            humio,
            stream,
        ),
        name=f"partition-{stream['partition']}",
        daemon=True,
    )
    t.start()

    log.info(f"Started thread stream for partition {stream['partition']}")

    return t


def parse_stream(stream):
    res = {
        "url": stream["dataFeedURL"],
        "token": stream["sessionToken"]["token"],
        "refresh_interval": int(stream["refreshActiveSessionInterval"]),
    }

    # Extract partition from stream URL
    res["partition"] = retrieve_partition_from_url(res["url"])

    res["refresh_delta"] = timedelta(seconds=(res["refresh_interval"] * 0.85))

    return res


def get_streams(falcon, args, partition=-1):
    if partition == -1:
        log.info("Getting available event streams")

    # I've encountered multiple times getting an empty resource list, this is to re-try in the case and
    # handling the graceful exit so end-user isn't stuck.
    streams_response = None
    retires = 1

    while not exit_event.is_set():
        app_id = args.app_id
        if args.appid_random != 0 and retires > args.appid_random:
            rnd_appid = random_app_id(args.app_id)
            log.info(
                f"Could not retrieve stream with App ID {args.app_id}, generating a random one {rnd_appid}."
            )
            app_id = rnd_appid

        streams = falcon.list_available_streams(app_id=app_id, format="json")
        log.debug(
            f"Got response for streams, we're getting partition {partition} : {json.dumps(streams)}"
        )
        streams_response = streams.get("body").get("resources")
        if streams["status_code"] != 200 or streams_response is None:
            log.error(
                f"Could not find any streams in response. "
                f"Make sure app id isn't used for multiple streams. Retrying..."
            )
            retires += 1
            exit_event.wait(args.retry_timer)
        else:
            break

    if exit_event.is_set():
        sys.exit(1)

    streams = [parse_stream(s) for s in streams_response]
    if partition == -1:
        log.info("Stream URL(s) received")
        return streams
    else:
        return [s for s in streams if s["partition"] == partition]


def app_run(args, falcon, humio):
    streams = get_streams(falcon, args)

    threads = {}
    for stream in streams:
        threads[stream["partition"]] = start_thread(args, falcon, humio, stream)

    while not exit_event.is_set():
        for p, t in threads.items():
            if not t.is_alive():
                log.error(f"Dead thread detected for partition {p}... Restarting...")
                stream = get_streams(falcon, args, partition=p)[0]
                threads[p] = start_thread(args, falcon, humio, stream)

        log.debug(
            "Checking threads ({})".format(", ".join([str(k) for k in threads.keys()]))
        )

        exit_event.wait(args.keepalive)


def app_prepare(args):
    threading.current_thread().name = "controller"

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

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    humio = {
        "header": {
            "Authorization": f"Bearer {args.humio_token}",
            "Content-Type": "application/json",
            "User-Agent": args.user_agent,
        },
    }

    if args.enrich:
        humio["metadata"] = {"@host": socket.gethostname(), "@stream": args.app_id}
        humio["event_keyword"] = "events"
        humio["url"] = urljoin(args.humio_url, "/api/v1/ingest/humio-structured")
    else:
        humio["event_keyword"] = "messages"
        humio["url"] = urljoin(args.humio_url, "/api/v1/ingest/humio-unstructured")

    app_run(args, falcon, humio)


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
        type=int,
        action="store",
        help="Maximum number of events to send in bulk",
    )

    advanced.add_argument(
        "--flush-wait-time",
        default=10,
        type=int,
        action="store",
        help="Maximum time to wait if bulk max size isn't reached",
    )

    advanced.add_argument(
        "--stream-timeout",
        default=60,
        type=int,
        action="store",
        help="Timeout for the event stream connection",
    )

    advanced.add_argument(
        "--retry-timer",
        default=300,
        type=int,
        action="store",
        help="How long to wait before retrieving streams between failures",
    )

    advanced.add_argument(
        "--appid-random",
        default=1,
        type=int,
        action="store",
        help="How many retries before going with random app id, 0 = disabled",
    )

    advanced.add_argument(
        "--keepalive",
        default=10,
        type=int,
        action="store",
        help="How often to verify threads are alive",
    )

    advanced.add_argument(
        "--exceptions",
        help="Dump exceptions, used on top of verbose, will cause multiline logs",
        action="store_true",
    )

    args = parser.parse_args()

    missing_args = []
    for arg in vars(args):
        env = os.environ.get(arg.upper())
        if env:
            if arg in (
                "bulk_max_size",
                "bulk_max_size",
                "stream_timeout",
                "retry_timer",
                "appid_random",
                "keepalive",
            ):
                env = int(env)
            elif arg in ("verbose", "enrich", "exceptions"):
                env = env.lower() in ("true", "1", "t")

            setattr(args, arg, env)

        if getattr(args, arg) is None:
            missing_args.append(arg.upper())

    if missing_args:
        log.error(f"Please set missing variables: {', '.join(missing_args)}")
        sys.exit(1)

    app_prepare(args)


if __name__ == "__main__":
    cli()
