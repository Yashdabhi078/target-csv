import sys
import argparse
import json
import collections

from singer.catalog import Catalog

logger = singer.get_logger()


class TargetHelper:
    @classmethod
    def emit_state(cls, state):
        if state is not None:
            line = json.dumps(state)
            logger.debug('Emitting state {}'.format(line))
            sys.stdout.write("{}\n".format(line))
            sys.stdout.flush()

    @classmethod
    def flatten(cls, d, parent_key='', sep='__'):
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(cls.flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, str(v) if type(v) is list else v))
        return dict(items)

    @classmethod
    def create_metadata_for_report(cls, schema, tap_stream_id):
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

    @classmethod
    def transform(cls, records, stream_mappings):
        fields_mapping = stream_mappings.get("fields_mapping", {})
        return {fields_mapping.get(k, k): r for k, r in records.items()}

    @classmethod
    def check_config(cls, config, required_keys):
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise Exception("Config is missing required keys: {}".format(missing_keys))

    @classmethod
    def load_json(cls, path):
        with open(path) as fil:
            return json.load(fil)

    @classmethod
    def parse_args(cls, required_config_keys):
        parser = argparse.ArgumentParser()

        parser.add_argument(
            '-c', '--config',
            help='Config file',
            required=True)

        parser.add_argument(
            '-s', '--state',
            help='State file')

        parser.add_argument(
            '-p', '--properties',
            help='Property selections: DEPRECATED, Please use --catalog instead')

        parser.add_argument(
            '--catalog',
            help='Catalog file')

        parser.add_argument(
            '-d', '--discover',
            action='store_true',
            help='Do schema discovery')

        parser.add_argument(
            '-m', '--fields-mapping',
            help='.json file path for streams fields mapping.',
            default=None)

        args = parser.parse_args()
        if args.config:
            setattr(args, 'config_path', args.config)
            args.config = cls.load_json(args.config)
        if args.state:
            setattr(args, 'state_path', args.state)
            args.state = cls.load_json(args.state)
        else:
            args.state = {}
        if args.properties:
            setattr(args, 'properties_path', args.properties)
            args.properties = cls.load_json(args.properties)
        if args.catalog:
            setattr(args, 'catalog_path', args.catalog)
            args.catalog = Catalog.load(args.catalog)

        cls.check_config(args.config, required_config_keys)

        return args
