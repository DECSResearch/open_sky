import configparser
from getpass import getpass
import os


def create_config():
    """
    Create a config file in the root dir to manage: 1.Opensky credentials 2. Project Root
    :return: None
    """
    # Creating a blank config file
    config = configparser.ConfigParser()
    # Adding Paths
    config['paths'] = {}
    config['paths']['ROOT'] = os.getcwd()

    # Creating a section for opensky
    config['open_sky'] = {}
    # The default host and port for opensky:
    config['open_sky']['host'] = "data.opensky-network.org"
    config['open_sky']['port'] = "2230"

    # Taking user input for opensky credentials
    config['open_sky']['usrnam'] = input("Pls enter opensky username and hit enter: ")
    config['open_sky']['psswrd'] = getpass()

    print("Writing config file...")
    # opening a new file of name "Config.ini to save the config details along with user entered details
    with open('config.ini', 'w') as configfile:
        config.write(configfile)
    configfile.close()

    print("Complete...")


if __name__ == "__main__":
    create_config()
