import paramiko
from setup import create_config
import os
import configparser

config = configparser.ConfigParser()
config.sections()
config.read('config.ini')
try:
    o_sky = config["open_sky"]
except KeyError:
    print("It looks like this program is running on this system for the first time.. please complete the setup "
          "process: ")
    create_config()

    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    o_sky = config["open_sky"]





def call_query(query, file_path):
    """
    :param query: The query to be passed tp opensky
    :param file_path: The path where the file is to saved
    :return: None
    """
    # opening an ssh client
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Load ssh keys
    ssh.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    # Make the connection
    ssh.connect("data.opensky-network.org",
                username=o_sky["usrnam"],
                password=o_sky['psswrd'],
                port=int(o_sky["port"]),
                banner_timeout=500
                )
    # The response from the server is saved in respective variables
    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("-q " + query, get_pty=True)
    # Opening a text file to save the printed outputs
    with open(file_path, 'a+') as the_file:
        for i in ssh_stdout:
            # print(i)
            the_file.write(i)

        the_file.close()
    # Closing the connection
    ssh.close()

    # Defining the path of the csv file
    csv_path = file_path.replace(".txt", ".csv")
    # Passing the command to process the txt file as a csv
    cmd = f"""cat {file_path} | grep "^|.*" | sed -e 's/\s*|\s*/,/g' -e 's/^,\|,$//g' -e 's/NULL//g' | awk '!seen[$0]++' >> {csv_path}"""
    # Running the command
    os.system(cmd)
