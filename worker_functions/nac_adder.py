import os
import numpy as np
import pandas as pd
from multiprocessing.pool import ThreadPool as Pool


def nac_and_nic_saver(nic_file_paths1, nac_file_paths1, unified_file_path1):

    def nac_inner_func(icao, nic_file_paths=nic_file_paths1,
                           nac_file_paths=nac_file_paths1, unified_file_path=unified_file_path1):

        # Filtering the over all df on icao
        ic_df = pd.read_csv(os.path.join(nic_file_paths, icao + ".csv"))

        # Getting the operational data file
        # Some instances of op data comes blank. This check is in place to ensure the same.
        try:
            op_data = pd.read_csv(os.path.join(nac_file_paths, icao + ".csv"))

        # If there is an error in reading the operational status data;
        except:
            # Add NaN's and save the file
            ic_df['nac_pos'] = np.NaN
            ic_df['nac_vert'] = np.NaN
            ic_df['nac_msg_count'] = np.NaN
            ic_df.to_csv(os.path.join(unified_file_path, icao + ".csv"))
            # Continue the loop
            return

        # Getting the operational data
        pos_df = op_data
        nac_position = []
        nacvertical = []
        nac_message_counts = []
        yes_count = 0
        inner_yes = 0
        inner_inner_yes = 0
        no_counter = 0

        for i in ic_df["lastposupdate"]:

            f = pos_df[((pos_df['maxtime'] == i) | (pos_df['mintime'] == i))]
            # print(f.columns)
            if f.shape[0] >= 1:
                # print(f.shape[0])
                yes_count += 1
                nacpos = f['positionnac'].values[-1]
                vnac = f['nacv'].values[-1]
                msg_count = f['msgcount'].values[-1]

                nac_position.append(nacpos)
                nacvertical.append(vnac)
                nac_message_counts.append(msg_count)
                # print(nic)
            else:
                f1 = pos_df[
                    ((pos_df['maxtime'].apply(lambda x: int(x)) == int(i)) |
                     (pos_df['mintime'].apply(lambda x: int(x)) == int(i))) |

                    ((pos_df['maxtime'].apply(lambda x: round(x, 0)) == round(i, 0)) |
                     (pos_df['mintime'].apply(lambda x: round(x, 0)) == round(i, 0)))]

                if f1.shape[0] >= 1:

                    inner_yes += 1
                    nacpos = f1['positionnac'].values[-1]
                    vnac = f1['nacv'].values[-1]
                    msg_count = f1['msgcount'].values[-1]

                    nac_position.append(nacpos)
                    nacvertical.append(vnac)
                    nac_message_counts.append(msg_count)

                else:
                    if (pos_df['maxtime'] - i).min() <= 2:
                        pos_data_idx = (pos_df['maxtime'] - i).idxmin()
                        f2 = pos_df.iloc[pos_data_idx]
                        nacpos = f2['positionnac']
                        vnac = f2['nacv']
                        msg_count = f2['msgcount']
                        inner_inner_yes += 1

                    elif (pos_df['mintime'] - i).min() <= 2:
                        pos_data_idx = (pos_df['mintime'] - i).idxmin()
                        f2 = pos_df.iloc[pos_data_idx]
                        nacpos = f2['positionnac']
                        vnac = f2['nacv']
                        msg_count = f2['msgcount']

                    else:
                        nacpos = np.NaN
                        vnac = np.NaN
                        msg_count = np.NaN
                        no_counter += 1

                    nac_position.append(nacpos)
                    nacvertical.append(vnac)
                    nac_message_counts.append(msg_count)

        ic_df['nac_pos'] = nac_position
        ic_df['nac_vert'] = nacvertical
        ic_df['nac_msg_count'] = nac_message_counts
        ic_df.to_csv(os.path.join(unified_file_path, icao+".csv"))




    pool_size = 5
    pool = Pool(pool_size)

    list_of_icaos = [i.replace(".csv", "") for i in os.listdir(nic_file_paths) if i.endswith(".csv")]
    for icao in list_of_icaos:
        pool.apply_async(nac_inner_func, (icao,))
    pool.close()
    pool.join()

