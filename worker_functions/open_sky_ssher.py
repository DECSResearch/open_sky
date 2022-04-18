import paramiko
import os
import configparser

config = configparser.ConfigParser()
config.sections()

config.read('config.ini')

o_sky = config["open_sky"]


def call_query(query, file_path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # ssh.load_system_host_keys()
    ssh.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    ssh.connect("data.opensky-network.org",
                username="Akzilla1010",
                password="Aks101090hay",
                port=2230,
                banner_timeout=500
                )

    ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("-q " + query, get_pty=True)

    tot_str = ''

    with open(file_path, 'a+') as the_file:
        for i in ssh_stdout:
            # print(i)
            the_file.write(i)

        the_file.close()
    ssh.close()
    csv_path = file_path.replace(".txt", ".csv")
    #os.system(f"sudo chown 777 {file_path}")
    cmd = f"""cat {file_path} | grep "^|.*" | sed -e 's/\s*|\s*/,/g' -e 's/^,\|,$//g' -e 's/NULL//g' | awk '!seen[$0]++' >> {csv_path}"""
    os.system(cmd)
