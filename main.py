from worker_functions.open_sky_ssher import call_query
import pandas as pd
from tqdm import tqdm
from worker_functions.nic_adder import add_nic
from worker_functions.nac_adder import nac_and_nic_saver
from worker_functions.epoch_time_converter import get_epoch_time
import os
import configparser

config = configparser.ConfigParser()

config.read('config.ini')
config['paths']['ROOT'] = os.getcwd()

R00t = config['paths']['ROOT']

start_time = ()
eng_time = ()
s_time = 1618945200
e_time = s_time + 120



s_hour = s_time - (s_time % 3600)
e_hour = e_time - (e_time % 3600)

### Below variables need to be added to make query forming much easier. Epoch time converter solved.
# start_epoch = get_epoch_time(start_time)
# end_epoch = get_epoch_time(eng_time)
# position = (1,)
# radius = 354


pos_data_query = f"SELECT * FROM state_vectors_data4 WHERE time>={s_time} AND time<={e_time} AND hour>={s_hour} " \
                 f"AND hour<={e_hour} AND lat>=26.956 AND lat<=38.956 AND lon>=-112.53733333333334 AND"\
                 " lon<=-100.53733333333334;"
save_dir_main = "/home/akshay.ramchandra/PycharmProjects/nac_nic_unifier/data/19apr2021_first/"

print(pos_data_query)

# Pass .txt in path to call_query, the .txt will be replaced to .csv and saved as a file in the same location
save_path = os.path.join(save_dir_main, "main_query.txt")
#call_query(pos_data_query, save_path)
query_base_path = os.path.dirname(save_path)
nic_dir = os.path.join(query_base_path, "nic_queried")

os.mkdir(nic_dir)
nac_dir = os.path.join(query_base_path, "nac_queried")
os.mkdir(nac_dir)
added_nic = os.path.join(query_base_path, "nic_added")
os.mkdir(added_nic)
unified = os.path.join(query_base_path, "unified")
os.mkdir(unified)
#
main_df = pd.read_csv(save_path.replace(".txt", ".csv"))
unique_icaos = main_df['icao24'].unique()

for unique_icao in tqdm(unique_icaos):
    filtered_df = main_df[main_df["icao24"] == unique_icao]
    start_time = filtered_df['lastposupdate'].min()
    end_time = filtered_df['lastposupdate'].max()
    h1 = start_time - (start_time % 3600)
    h2 = end_time - (end_time % 3600)

    nic_query = f"select * FROM position_data4 WHERE icao24 ='{unique_icao}' " \
                f"AND maxtime>={start_time} AND maxtime<={end_time} AND hour>={h1} AND hour<={h2};"

    nic_save_path_txt = os.path.join(nic_dir, unique_icao + ".txt")

    call_query(nic_query, nic_save_path_txt)

    nac_query = f"select * FROM operational_status_data4 WHERE icao24 ='{unique_icao}' AND maxtime>={start_time} AND " \
                f"maxtime<={end_time} AND hour>={h1} AND hour<={h2};"

    nac_save_path_txt = os.path.join(nac_dir, unique_icao + ".txt")

    call_query(nac_query, nac_save_path_txt)

add_nic(main_df, nic_dir, added_nic)
nac_and_nic_saver(added_nic, nac_dir, unified)

if __name__ == '__main__':
    pass

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
