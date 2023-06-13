import yaml

from common.Singleton import Singleton


def load_conf_yaml(name: str, mode='r') -> dict:
    with open(name, mode) as stream:
        return yaml.safe_load(stream)


class Config(metaclass=Singleton):

    def __init__(self, config_file="app.yml"):
        self.__config_file = config_file
        self.__conf = load_conf_yaml(config_file)

    def conf(self):
        return self.__conf


if __name__ == '__main__':
    conf = load_conf_yaml('app.yml')
    print(type(conf))
    print(conf)
    print(f'app name: {conf.get("common").get("name")}')