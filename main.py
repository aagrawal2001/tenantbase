from data_layer import DataLayer
from schema import create_schema
from memcache_receiver import MemcacheFactory
from twisted.internet import reactor
import sys

DEFAULT_PORT = 11211


def install(db):
    create_schema(db)


def serve(db):
    data_layer = DataLayer(db)
    reactor.listenTCP(DEFAULT_PORT, MemcacheFactory(data_layer))
    reactor.run()


def show(db):
    data_layer = DataLayer(db)
    for row in data_layer.get_all_values():
        print("Key: {key}, Flags: {flags}, Value: {value}".format(**row))


MODE_INSTALL = "install"
MODE_SERVE = "serve"
MODE_SHOW = "show"
MODE_MAP = {
    MODE_INSTALL: install,
    MODE_SERVE: serve,
    MODE_SHOW: show,
}


def print_usage_and_exit():
    print("Usage: main.py show|serve|install <database name>")
    sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print_usage_and_exit()

    mode = sys.argv[1]
    if mode not in MODE_MAP:
        print_usage_and_exit()

    MODE_MAP[mode](sys.argv[2])


if __name__ == '__main__':
    main()
