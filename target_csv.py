#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import csv
import threading
import http.client
import urllib
from datetime import datetime
import collections

import pandas
from pandas.errors import EmptyDataError
import pkg_resources

from jsonschema.validators import Draft4Validator
import singer
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


def transform(records, stream_mappings):
    field_mappings = stream_mappings.get("field_mappings", {})
    return {field_mappings.get(k, k): r for k, r in records.items()}


def persist_messages(delimiter, quotechar, messages, destination_path, field_mapping_file=None):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    mappings = {}

    if field_mapping_file:
        with open(field_mapping_file) as input_json:
            mappings = json.load(input_json)

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
            # o = json.loads(message + "}")
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            validators[o['stream']].validate(o['record'])
            stream_mapping = mappings.get(o['stream'], {})

            filename = stream_mapping.get("to", o['stream'] + '-' + now) + '.csv'
            filename = os.path.expanduser(os.path.join(destination_path, filename))
            file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0

            flattened_record = flatten(o['record'])
            transformed_records = transform(flattened_record, stream_mapping)
            if o['stream'] not in headers and not file_is_empty:
                with open(filename, 'r') as csvfile:
                    reader = csv.reader(csvfile,
                                        delimiter=delimiter,
                                        quotechar=quotechar)
                    first_line = next(reader)
                    headers[o['stream']] = first_line if first_line else transformed_records.keys()
            elif file_is_empty:
                headers[o['stream']] = transformed_records.keys()

            with open(filename, 'a') as csvfile:
                writer = csv.DictWriter(csvfile,
                                        headers[o['stream']],
                                        extrasaction='ignore',
                                        delimiter=delimiter,
                                        quotechar=quotechar)
                if file_is_empty:
                    writer.writeheader()

                writer.writerow(transformed_records)

            state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            key_properties[stream] = o['key_properties']
        else:
            logger.warning("Unknown message type {} in message {}"
                           .format(o['type'], o))

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-csv',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def read_csvs(path):
    streams = {}
    list_csv = [file for file in os.listdir(path) if file[-4:] == ".csv"]
    for file in list_csv:
        try:
            df = pandas.read_csv(path + file, sep='\t')
            streams[file[:-4]] = Schema.from_dict({
                "type": ["null", "object"],
                "properties": {k: {"type": ["null", "string"]} for k in df.keys()}
            })
        except EmptyDataError:
            logger.info("Skipping. %s is empty.", file)
    return streams


def create_metadata_for_report(schema, tap_stream_id):
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available"}}]

    for key in schema.properties:
        if "object" in schema.properties.get(key).type:
            for prop in schema.properties.get(key).properties:
                inclusion = "available"
                mdata.extend([{
                    "breadcrumb": ["properties", key, "properties", prop],
                    "metadata": {"inclusion": inclusion}
                }])
        else:
            inclusion = "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion}})

    return mdata


def discover(config):
    path = config["destination_path"]
    raw_schemas = read_csvs(path)
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(schema, stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=[],
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-d', '--discover', help='Do discovery', action='store_true')
    parser.add_argument('-m', '--field-mappings', help='.json file path for streams fields mapping.', default=None)
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    if args.discover:
        catalog = discover(config)
        catalog.dump()
    else:
        input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

        state = persist_messages(config.get('delimiter', ','),
                                 config.get('quotechar', '"'),
                                 input_messages,
                                 config.get('destination_path', ''),
                                 args.field_mappings)

        emit_state(state)
        logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
