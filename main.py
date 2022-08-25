import sys
from time import sleep
from worker_functions.open_sky_ssher import call_query
import pandas as pd
from tqdm import tqdm
from worker_functions.nic_adder import add_nic
from worker_functions.nac_adder import nac_and_nic_saver
from recaller import query_re_caller
from worker_functions.epoch_time_converter import get_epoch_time

import os
import configparser

config = configparser.ConfigParser()

config.read('config.ini')
config['paths']['ROOT'] = os.getcwd()

R00t = config['paths']['ROOT']

strttime = ()
ndtime = ()
s_time = 1618945200
e_time = s_time + 120

s_hour = s_time - (s_time % 3600)
e_hour = e_time - (e_time % 3600)

### Below variables need to be added to make query forming much easier. Epoch time converter solved.
# start_epoch = get_epoch_time(start_time)
# end_epoch = get_epoch_time(eng_time)
# position = (1,)
# radius = 354

# Dummy Sample query
query = f"SELECT * FROM state_vectors_data4 WHERE time>={s_time} AND time<={e_time} AND hour>={s_hour} " \
        f"AND hour<={e_hour} AND lat>=26.956 AND lat<=38.956 AND lon>=-112.53733333333334 AND" \
        " lon<=-100.53733333333334;"


def query_caller(query, save_folder_name):
    """
    Reads the main df and takes it from there. If maindf is not downloaded, it will not work.

    :param query: query as a string for position data4 table of opensky
    :param save_folder_name: The folder name where the data will be saved. This will be treated as the root dir for
           where the downloaded files wll be saved after making new directories.
    :return: None
    """
    pos_data_query = query
    #save_dir_main = "/home/akshay.ramchandra/PycharmProjects/nac_nic_unifier/data/19apr2021_first/"
    save_dir_main = os.path.join(R00t, "data", save_folder_name)
    print("I'm making the save dir for the query  data..")
    os.mkdir(save_dir_main)
    print("Received query: ")
    print(pos_data_query)

    # Pass .txt in path to call_query, the .txt will be replaced to .csv and saved as a file in the same location
    save_path = os.path.join(save_dir_main, "main_query.txt")
    # Calling the query and saving the data to a text file.
    print("Calling the query: ")
    call_query(pos_data_query, save_path)
    print("completed calling the main query, now moving on to nic and nac query")
    # Defining paths
    query_base_path = os.path.dirname(save_path)
    nic_dir = os.path.join(query_base_path, "nic_queried")
    nac_dir = os.path.join(query_base_path, "nac_queried")
    added_nic = os.path.join(query_base_path, "nic_added")
    unified = os.path.join(query_base_path, "unified")

    # Making the directories
    os.mkdir(nic_dir)
    os.mkdir(nac_dir)
    os.mkdir(added_nic)
    os.mkdir(unified)

    # Reading the positiondata4 csv
    main_df = pd.read_csv(save_path.replace(".txt", ".csv"))
    # Getting the unique icaos from the downloaded data
    unique_icaos = main_df['icao24'].unique()

    # Iterating ICAOS
    for unique_icao in tqdm(unique_icaos):
        # Filtering the pos4 data to get a sub data of iterated icao
        filtered_df = main_df[main_df["icao24"] == unique_icao]
        # Getting span of times - Start and end
        start_time = filtered_df['lastposupdate'].min()
        end_time = filtered_df['lastposupdate'].max()
        # Calculate the "HOUR" parameter for the query
        h1 = start_time - (start_time % 3600)
        h2 = end_time - (end_time % 3600)
        ################################################################################################################
        ####################################### NIC Query Formation ####################################################
        ################################################################################################################
        # Forming the Navigation Integrity Query
        nic_query = f"select * FROM position_data4 WHERE icao24 ='{unique_icao}' " \
                    f"AND maxtime>={start_time} AND maxtime<={end_time} AND hour>={h1} AND hour<={h2};"

        # Defining the path where the queried NIC info will be saved
        nic_save_path_txt = os.path.join(nic_dir, unique_icao + ".txt")
        # Calling the query
        call_query(nic_query, nic_save_path_txt)

        ################################################################################################################
        ####################################### NAC Query Formation ####################################################
        ################################################################################################################

        nac_query = f"select * FROM operational_status_data4 WHERE icao24 ='{unique_icao}' AND maxtime>={start_time} AND " \
                    f"maxtime<={end_time} AND hour>={h1} AND hour<={h2};"

        nac_save_path_txt = os.path.join(nac_dir, unique_icao + ".txt")

        call_query(nac_query, nac_save_path_txt)
    ####################################################################################################################
    ########################################### Unifying NAC NIC Query #################################################
    ####################################################################################################################
    add_nic(main_df, nic_dir, added_nic)
    nac_and_nic_saver(added_nic, nac_dir, unified)


if __name__ == '__main__':
    maxreconns = 10
    start_epoch_time = int(input("Enter start epoch time: "))
    end_epoch_time = int(input("End epoch time: "))
    folder_name = input("Enter the name of the folder in which the data needs to be saved(this will be created in the "
                        "data folder): ")
    save_dir_main = os.path.join(R00t, "data", folder_name)
    if os.path.exists(save_dir_main):
        approved = False
        while not approved:
            folder_name = input(
                "Folder name already exists Pls choose another name: ")
            save_dir_main = os.path.join(R00t, "data", folder_name)
            if not os.path.exists(save_dir_main):
                approved = True
            else:
                pass

    else:
        pass

    start_epoch_hour = start_epoch_time % 3600
    end_epoch_hour = end_epoch_time % 3600

    query = f"SELECT * FROM state_vectors_data4 WHERE time >= {start_epoch_time} AND time <= {end_epoch_time} AND hour >= {start_epoch_hour} AND hour <= {end_epoch_hour};"
    # query, folder_name = (sys.argv[1], sys.argv[2])
    try:
        query_caller(query, folder_name)
    except Exception as e:
        print(f"Normal Try, error encountered: {e}")
        recon_counter = 0
        while True and recon_counter < maxreconns:
            recon_counter += 1
            try:
                sleep(30)
                query_re_caller(query, folder_name)
                break
            except Exception as e:
                print(f"Retry #{recon_counter}; Error: {e}")
                print("Attempting Retry...")
