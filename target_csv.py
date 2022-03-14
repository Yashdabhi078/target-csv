#!/usr/bin/env python3

import io
import os
import sys
import json
import csv
from datetime import datetime

import pandas
from pandas.errors import EmptyDataError

from jsonschema.validators import Draft4Validator
import singer
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from target_helper import TargetHelper

logger = singer.get_logger()
REQUIRED_CONFIG_KEYS = []


def persist_messages(delimiter, quotechar, messages, destination_path, field_mapping_file=None):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    mappings = {}

    logger.info("Read fields mapping file, %s", field_mapping_file)
    if field_mapping_file:
        with open(field_mapping_file) as input_json:
            mappings = json.load(input_json)
    logger.info("mappings. %s", mappings)
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

            flattened_record = TargetHelper.flatten(o['record'])
            transformed_records = TargetHelper.transform(flattened_record, stream_mapping)
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


def read_csvs(path):
    streams = {}
    try:
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
    except FileNotFoundError:
        pass
    return streams


def discover(config):
    path = config["destination_path"]
    raw_schemas = read_csvs(path)
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = TargetHelper.create_metadata_for_report(schema, stream_id)
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
    args = TargetHelper.parse_args(REQUIRED_CONFIG_KEYS)

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    if args.discover:
        catalog = discover(config)
        catalog.dump()
    else:
        input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

        state = persist_messages(config.get('delimiter', ','),
                                 config.get('quotechar', '"'),
                                 input_messages,
                                 config.get('destination_path', ''),
                                 args.fields_mapping)

        TargetHelper.emit_state(state)
        logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
