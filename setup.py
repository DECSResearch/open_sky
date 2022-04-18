import configparser
from getpass import getpass
import os


def create_config():
    config = configparser.ConfigParser()
    config['paths'] = {}
    config['paths']['ROOT'] = os.getcwd()

    config['open_sky'] = {}
    config['open_sky']['host'] = "data.opensky-network.org"
    config['open_sky']['port'] = "2230"

    config['open_sky']['usrnam'] = input("Pls enter opensky username and hit enter: ")
    config['open_sky']['psswrd'] = getpass()
    print("Writing config file...")
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    print("Complete...")


if __name__ == "__main__":
    create_config()
