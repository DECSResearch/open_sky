import os
import numpy as np
import pandas as pd


def add_nic(df, nic_df_path, nic_added_path):
    """

    :param df: the main data frame
    :param nic_df_path: path to save the states_vector_data4 after adding nic values
    :return: None
    """
    unique_icaos = df['icao24'].unique()
    # unique_icaos = [i for i in unique_icaos if i not in j.replace(".csv", "") for j in os.listdir(nic_added_path)]
    #unique_icaos = [i.replace(".csv", "") for i in os.listdir(nic_df_path) if i.endswith(".csv")]
    for unique_icao in unique_icaos:

        ic_df = df[df['icao24'] == unique_icao]
        pos_df = pd.read_csv(os.path.join(nic_df_path, f"{unique_icao}" + ".csv"))
        print(unique_icao, pos_df.columns)

        nic_values = []
        nic_message_counts = []

        for i in ic_df["lastposupdate"]:

            f = pos_df[((pos_df['maxtime'] == i) | (pos_df['mintime'] == i))]

            if f.shape[0] >= 1:
                # print(f.shape[0])
                # yes_count += 1
                nic = f['nic'].values[-1]
                msg_count = f['msgcount'].values[-1]

                nic_values.append(nic)
                nic_message_counts.append(msg_count)
                # print(nic)
            else:
                f1 = pos_df[
                    ((pos_df['maxtime'].apply(lambda x: int(x)) == int(i)) |
                     (pos_df['mintime'].apply(lambda x: int(x)) == int(i))) |

                    ((pos_df['maxtime'].apply(lambda x: round(x, 0)) == round(i, 0)) |
                     (pos_df['mintime'].apply(lambda x: round(x, 0)) == round(i, 0)))]

                if f1.shape[0] >= 1:

                    # inner_yes += 1
                    nic = f1['nic'].values[-1]
                    msg_count = f1['msgcount'].values[-1]

                    nic_values.append(nic)
                    nic_message_counts.append(msg_count)

                else:
                    if (pos_df['maxtime'] - i).min() <= 2:
                        pos_data_idx = (pos_df['maxtime'] - i).idxmin()
                        f2 = pos_df.iloc[pos_data_idx]
                        nic = f2['nic']
                        msg_count = f2['msgcount']
                        # inner_inner_yes += 1


                    elif (pos_df['mintime'] - i).min() <= 2:
                        pos_data_idx = (pos_df['mintime'] - i).idxmin()
                        f2 = pos_df.iloc[pos_data_idx]
                        nic = f2['nic']
                        msg_count = f2['msgcount']

                    else:
                        nic = np.NaN
                        msg_count = np.NaN
                        # no_counter += 1

                    nic_values.append(nic)
                    nic_message_counts.append(msg_count)


        ic_df['nic_value'] = nic_values
        ic_df['nic_messages'] = nic_message_counts
        nic_df_path_to_save = os.path.join(nic_added_path, unique_icao + ".csv")
        ic_df.to_csv(nic_df_path_to_save)
