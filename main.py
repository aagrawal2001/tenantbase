from data_layer import DataLayer
from memcache_receiver import MemcacheFactory
from twisted.internet import reactor
import sys

DEFAULT_PORT = 11211


def serve(data_layer):
    reactor.listenTCP(DEFAULT_PORT, MemcacheFactory(data_layer))
    reactor.run()


def show(data_layer):
    for row in data_layer.get_all_values():
        print(
            "Key: %s, Flags: %d, Value: %s" %
            (row["key"], row["flags"], row["value"])
        )


MODE_SERVE = "serve"
MODE_SHOW = "show"
MODE_MAP = {
    MODE_SERVE: serve,
    MODE_SHOW: show,
}


def print_usage_and_exit():
    print("Usage: main.py show|serve <database name>")
    sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print_usage_and_exit()

    mode = sys.argv[1]
    if mode not in MODE_MAP:
        print_usage_and_exit()

    db = sys.argv[2]
    data_layer = DataLayer(db)
    MODE_MAP[mode](data_layer)


if __name__ == '__main__':
    main()
