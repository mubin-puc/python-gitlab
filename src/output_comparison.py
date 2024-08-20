import pandas as pd
import numpy as np
import os
from src.common_utils import download_json_from_s3, upload_comparison_output_file_to_s3
from conf.logger_config import log_info, log_msg, log_error, log_success


# function to compare Product Lines from Legacy vs Modernized DMA json file
def compare_product_lines(df_productline_legacy, df_productline_modernized):
    """
    :param df_productline_legacy: str
    :param df_productline_modernized: str
    :return: csv_filename: str
    """
    missing_in_legacy = np.setdiff1d(df_productline_modernized, df_productline_legacy)
    missing_in_modernized = np.setdiff1d(df_productline_legacy, df_productline_modernized)
    if missing_in_legacy.size > 0 or missing_in_modernized.size > 0:
        df_missing_productlines_legacy = pd.DataFrame({
            "Type": "DMA_Legacy",
            "ProductLine": missing_in_legacy,
            "Status": "Not Found"
        })
        df_missing_productlines_modernized = pd.DataFrame({
            "Type": "DMA_Modernized",
            "ProductLine": missing_in_modernized,
            "Status": "Not Found"
        })

        df_pl_difference = pd.concat([df_missing_productlines_legacy, df_missing_productlines_modernized])
        return df_pl_difference
    else:

        df_pl = pd.DataFrame(
            {'Legacy_ProductLines': df_productline_legacy, 'Modernized_ProductLines': df_productline_modernized,
             "Status": "Product Lines found both in Legacy and K8s"})
        return df_pl


# function to compare Package Names from Legacy vs Modernized DMA json file
def compare_package_names_for_productline(df_legacy, df_modernized, df_productline_legacy, df_productline_modernized):
    """
    :param df_legacy: str
    :param df_modernized: str
    :param df_productline_legacy: str
    :param df_productline_modernized: str
    :return: csv_filename
    """
    productlines_to_compare = np.union1d(df_productline_modernized, df_productline_legacy)
    # pl stands for product line
    # pi stands for packageid
    df_result_value_in_both = pd.DataFrame()
    for eachProductLine in productlines_to_compare:
        df_package_names_legacy = df_legacy.loc[
            (df_legacy["Product Line"] == eachProductLine, ['Product Line', 'Package Name', 'Package ID'])]
        df_package_names_modernized = df_modernized.loc[
            (df_modernized["Product Line"] == eachProductLine, ['Product Line', 'Package Name', 'Package ID'])]

        if not pd.isnull(df_package_names_legacy['Package ID']).all() and not pd.isnull(
                df_package_names_modernized['Package ID']).all():
            merged_df = pd.merge(df_package_names_legacy, df_package_names_modernized,
                                 on=['Product Line', 'Package Name', 'Package ID'],
                                 how='outer', indicator="Status")
            merged_df['Status'] = merged_df['Status'].map(
                {'left_only': 'only found in Legacy', 'right_only': 'only found in k8s',
                 'both': 'found in both Legacy and K8s'})
            df_result_value_in_both = pd.concat([df_result_value_in_both, merged_df], ignore_index=True)
            return df_result_value_in_both

# function to compare Signal Data from Legacy vs Modernized DMA json file
def compare_signal_data(df_legacy, df_modernized, df_package_id_legacy, df_package_id_modernized):
    """
    :param df_legacy: str
    :param df_modernized: str
    :param df_package_id_legacy: str
    :param df_package_id_modernized: str
    :return: csv_filename: str
    """
    package_id_list = np.union1d(df_package_id_legacy, df_package_id_modernized)
    signal_data_params = ['siteTime', 'tagName', 'tagAlias', 'value']
    ignore_tag_names_list = ['DMA Task Run Completed']
    df_result_value_in_both = pd.DataFrame()
    df_result_value_in_legacy = pd.DataFrame()
    df_result_value_in_modernized = pd.DataFrame()
    df_result_value_null = pd.DataFrame()
    df_summary_signaldata_result_both = pd.DataFrame()
    df_summary_signaldata_result_legacy = pd.DataFrame()
    df_summary_signaldata_result_modernized = pd.DataFrame()
    df_summary_signaldata_result_null = pd.DataFrame()
    for eachPackageID in package_id_list:
        df_signaldata_legacy = df_legacy.loc[(df_legacy["Package ID"] == eachPackageID,
                                              ['Product Line', 'Package Name', 'Package ID', 'Signal Data'])]
        df_signaldata_modernized = df_modernized.loc[(
            df_modernized["Package ID"] == eachPackageID,
            ['Product Line', 'Package Name', 'Package ID', 'Signal Data'])]

        if not pd.isnull(df_signaldata_modernized['Signal Data'].explode()).all() and not pd.isnull(
                df_signaldata_legacy['Signal Data'].explode()).all():


            list_of_signaldata_legacy = df_signaldata_legacy['Signal Data'].explode()
            new_df_signaldata_legacy = pd.json_normalize(list_of_signaldata_legacy)
            filtered_df_signaldata_legacy = new_df_signaldata_legacy[signal_data_params][~new_df_signaldata_legacy['tagName'].isin(ignore_tag_names_list)]
            filtered_df_signaldata_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signaldata_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signaldata_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            list_of_signaldata_modernized = df_signaldata_modernized['Signal Data'].explode()
            new_df_signaldata_modernized = pd.json_normalize(list_of_signaldata_modernized)
            filtered_df_signaldata_modernized = new_df_signaldata_modernized[signal_data_params][~new_df_signaldata_modernized['tagName'].isin(ignore_tag_names_list)]
            filtered_df_signaldata_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signaldata_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signaldata_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            exclude_tags_for_value_match = ['DMA Run Duration']
            df1 = filtered_df_signaldata_legacy[~filtered_df_signaldata_legacy['tagName'].isin(exclude_tags_for_value_match)].reset_index()
            # df1 = filtered_df_signaldata_legacy.reset_index()
            df1['value'] = pd.to_numeric(df1['value'], errors='coerce')

            df2 = filtered_df_signaldata_modernized[~filtered_df_signaldata_modernized['tagName'].isin(exclude_tags_for_value_match)].reset_index()
            # df2 = filtered_df_signaldata_modernized.reset_index()
            df2['value'] = pd.to_numeric(df2['value'], errors='coerce')

            # merged_df = pd.merge(df1, df2, how='outer', indicator=True)
            merged_df = pd.merge(df1, df2,
                                 on=['Product Line', 'Package Name', 'Package ID', 'siteTime', 'tagName', 'tagAlias'],
                                 how='outer', indicator=True)

            # diff_values = merged_df[merged_df['_merge']!='both']
            merged_df['Status'] = merged_df['_merge'].map(
                {'left_only': 'legacy', 'right_only': 'K8s', 'both': 'both'})

            merged_df.index = merged_df.index + 1

            rows_legacy = []
            rows_modernized = []
            rows_both = []

            for index, row in merged_df.iterrows():
                if row['Status'] == 'legacy':
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'siteTime',
                         row['siteTime'], "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'tagName',
                         row['tagName'],
                         "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'tagAlias',
                         row['tagAlias'], "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'value',
                         row['value_x'], "-",
                         'only found in legacy'])
                elif row['Status'] == 'K8s':
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_y'], 'siteTime', "-",
                         row['siteTime'], 'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_y'], 'tagName', "-",
                         row['tagName'], 'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_y'], 'tagAlias', "-",
                         row['tagAlias'], 'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_y'], 'value', "-",
                         row['value_y'],
                         'only found in k8s'])
                else:
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'siteTime',
                         row['siteTime'], row['siteTime'], 'output matched'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'tagName',
                         row['tagName'],
                         row['tagName'], 'output matched'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'tagAlias',
                         row['tagAlias'], row['tagAlias'], 'output matched'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], row['index_x'], 'value',
                         row['value_x'],
                         row['value_y'], 'output matched'])

            rows_result = rows_legacy + rows_modernized + rows_both
            result_df = pd.DataFrame(rows_result, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                           'Parameter', 'Legacy', 'K8s', 'Status'])
            result_df.to_csv('result_df.csv')
            result_df.fillna("-", inplace=True)

            # filtered_result_df = result_df[~result_df['tagName'].isin(exclude_tags_for_value_match)]
            filtered_df_value_signaldata_both = result_df[(result_df['Status'] == 'output matched') &
                                                                   (result_df['Parameter'] == 'value')]
            total_rows = filtered_df_value_signaldata_both.shape[0]
            matches = filtered_df_value_signaldata_both[
                filtered_df_value_signaldata_both['Legacy'] == filtered_df_value_signaldata_both['K8s']].shape[0]
            if matches+total_rows==0:
                percent_match = "N/A"
            else:
                percent_match = (matches / total_rows) * 100
            count_match = (len(df2) / len(df1)) * 100 if len(df2) < len(df1) \
                else (len(df1) / len(df2)) * 100

            df_summary_signal_data_both = pd.DataFrame(
                {
                    'Product Line':
                        df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                    'Package Name':
                        df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                    'Package ID': df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                        0],
                    'Legacy Signal Datapoints': len(df1),
                    'K8s Signal Datapoints': len(df2),
                    'Common Signal Datapoints': total_rows,
                    'Value Match %': [percent_match],
                    'Count Match %': [count_match]
                }
            )
            df_summary_signaldata_result_both = pd.concat(
                [df_summary_signaldata_result_both, df_summary_signal_data_both], ignore_index=True)
            df_result_value_in_both = pd.concat([df_result_value_in_both, result_df], ignore_index=True)

        elif pd.isnull(df_signaldata_modernized['Signal Data'].explode()).all() and not pd.isnull(
                df_signaldata_legacy['Signal Data'].explode()).all():

            list_of_signaldata_legacy = df_signaldata_legacy['Signal Data'].explode()
            new_df_signaldata_legacy = pd.json_normalize(list_of_signaldata_legacy)
            filtered_df_signaldata_legacy = new_df_signaldata_legacy[signal_data_params][~new_df_signaldata_legacy['tagName'].isin(ignore_tag_names_list)]
            filtered_df_signaldata_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signaldata_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signaldata_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_legacy = []
            for index, row in filtered_df_signaldata_legacy.iterrows():
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                    row['siteTime'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName',
                                    row['tagName'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias',
                                    row['tagAlias'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'value',
                                    row['value'], "-", 'only found in legacy'])

            result_df = pd.DataFrame(rows_legacy, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                           'Parameter', 'Legacy', 'K8s', 'Status'])
            result_df.fillna("-", inplace=True)
            # filtered_df_valueSignalData_legacy = result_df[result_df['Parameter'] == 'value']
            # total_rows = filtered_df_valueSignalData_legacy.shape[0]
            # matches = filtered_df_valueSignalData_legacy[filtered_df_valueSignalData_legacy['Legacy'] ==
            #                                               filtered_df_valueSignalData_legacy['K8s']].shape[0]
            # percent_match = (matches / total_rows) * 100
            df_legacy_count = len(filtered_df_signaldata_legacy)
            count_match = (0 / df_legacy_count) * 100 if df_legacy_count != 0 else "N/A"
            df_summary_signal_data_legacy = pd.DataFrame(
                {
                    'Product Line':
                        df_legacy.loc[(df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                    'Package Name':
                        df_legacy.loc[(df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                    'Package ID': df_legacy.loc[(df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[
                        0],
                    'Legacy Signal Datapoints': len(filtered_df_signaldata_legacy),
                    'K8s Signal Datapoints': 0,
                    'Common Signal Datapoints': 0,
                    'Value Match %': ["N/A"],
                    'Count Match %': [count_match]
                }
            )
            df_summary_signaldata_result_legacy = pd.concat(
                [df_summary_signaldata_result_legacy, df_summary_signal_data_legacy], ignore_index=True)
            df_result_value_in_legacy = pd.concat([df_result_value_in_legacy, result_df], ignore_index=True)

        elif pd.isnull(df_signaldata_legacy['Signal Data'].explode()).all() and not pd.isnull(
                df_signaldata_modernized['Signal Data'].explode()).all():
            list_of_signaldata_modernized = df_signaldata_modernized['Signal Data'].explode()
            new_df_signaldata_modernized = pd.json_normalize(list_of_signaldata_modernized)
            filtered_df_signaldata_modernized = new_df_signaldata_modernized[signal_data_params][~new_df_signaldata_modernized['tagName'].isin(ignore_tag_names_list)]
            filtered_df_signaldata_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signaldata_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signaldata_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_modernized = []
            for index, row in filtered_df_signaldata_modernized.iterrows():
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                        "-", row['siteTime'], 'only found in k8s'])
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName',
                                        "-", row['tagName'], 'only found in k8s'])
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias',
                                        "-", row['tagAlias'], 'only found in k8s'])
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'value',
                                        "-", row['value'], 'only found in k8s'])

            result_df = pd.DataFrame(rows_modernized, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                               'Parameter', 'Legacy', 'K8s', 'Status'])
            result_df.fillna("-", inplace=True)

            # filtered_df_valueSignalData_modernized = result_df[result_df['Parameter'] == 'value']
            # total_rows = filtered_df_valueSignalData_modernized.shape[0]
            # matches = filtered_df_valueSignalData_modernized[filtered_df_valueSignalData_modernized['Legacy']
            #                                                  == filtered_df_valueSignalData_modernized['K8s']].shape[0]
            # percent_match = (matches / total_rows) * 100
            df_modernized_count = len(filtered_df_signaldata_modernized)
            count_match = (0 / df_modernized_count) * 100 if df_modernized_count != 0 else "N/A"
            df_summary_signal_data_modernized = pd.DataFrame(
                {
                    'Product Line':
                        df_modernized.loc[(df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                    'Package Name':
                        df_modernized.loc[(df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                    'Package ID': df_modernized.loc[(df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[
                        0],
                    'Legacy Signal Datapoints': 0,
                    'K8s Signal Datapoints': len(filtered_df_signaldata_modernized),
                    'Common Signal Datapoints': 0,
                    'Value Match %': ["N/A"],
                    'Count Match %': [count_match]
                }
            )
            df_summary_signaldata_result_modernized = pd.concat(
                [df_summary_signaldata_result_modernized, df_summary_signal_data_modernized], ignore_index=True)

            df_result_value_in_modernized = pd.concat([df_result_value_in_modernized, result_df], ignore_index=True)

        else:
            product_line = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Product Line'].iloc[0]
            package_name = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Package Name'].iloc[0]
            package_id = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Package ID'].iloc[0]
            rows = []
            rows.append([product_line, package_name, package_id, "-", 'siteTime', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'tagName', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'tagAlias', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'value', "-", "-",
                         'No data found in both Legacy and K8s'])
            df_result = pd.DataFrame(rows, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                    'Parameter', 'Legacy', 'K8s', 'Status'])

            df_summary_agent_msg_data = pd.DataFrame({
                'Product Line': product_line,
                'Package Name': package_name,
                'Package ID': package_id,
                'Legacy Signal Datapoints': 0,
                'K8s Signal Datapoints': 0,
                'Common Signal Datapoints': 0,
                'Value Match %': ["N/A"],
                'Count Match %': ["N/A"]
            })
            df_result_value_null = pd.concat([df_result_value_null, df_result], ignore_index=False)
            df_summary_signaldata_result_null = pd.concat(
                [df_summary_signaldata_result_null, df_summary_agent_msg_data],
                ignore_index=False)

    signaldata_result_df = pd.concat(
        [df_result_value_in_both, df_result_value_in_legacy, df_result_value_in_modernized, df_result_value_null])
    signaldata_result_df.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    signaldata_result_df.reset_index(drop=True, inplace=True)

    summary_table_signaldata = pd.concat([df_summary_signaldata_result_legacy, df_summary_signaldata_result_modernized,
                                          df_summary_signaldata_result_both, df_summary_signaldata_result_null],
                                         ignore_index=True)
    summary_table_signaldata.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    # signaldata_csv_file = r'output/SignalData_output.csv'
    # signaldata_result_df.to_csv(signaldata_csv_file, index=True)
    return signaldata_result_df, summary_table_signaldata


# function to compare Event Data from Legacy vs Modernized DMA json file
def compare_event_data(df_legacy, df_modernized, df_package_id_legacy, df_package_id_modernized):
    """
    :param df_legacy: str
    :param df_modernized: str
    :param df_package_id_legacy: str
    :param df_package_id_modernized: str
    :return: csv_filename: str
    """
    package_id_list = np.union1d(df_package_id_legacy, df_package_id_modernized)
    event_data_ignore_params = {'Data Received', "Events Received"}
    event_datadf_ignore_columns = ['tagId', 'id', "trainId", "downtime", "assemblyName", "eventCategoryId",
                                   "eventDescription",
                                   "currentStateId", "isExternal", "activeTimeStamp",
                                   "eventStateIndicatorId", "tagIdentifier", "customerTagAlias", "tagAlias"]
    df_result_value_in_both = pd.DataFrame()
    df_result_value_in_legacy = pd.DataFrame()
    df_result_value_in_modernized = pd.DataFrame()
    df_result_value_null = pd.DataFrame()
    df_summary_event_data_result_both = pd.DataFrame()
    df_summary_event_data_result_legacy = pd.DataFrame()
    df_summary_event_data_result_modernized = pd.DataFrame()
    df_summary_event_data_result_null = pd.DataFrame()
    for eachPackageID in package_id_list:
        df_event_data_legacy = df_legacy.loc[(df_legacy["Package ID"] == eachPackageID,
                                              ['Product Line', 'Package Name', 'Package ID', 'Event Data'])]
        df_event_data_modernized = df_modernized.loc[(df_modernized["Package ID"] == eachPackageID,
                                                      ['Product Line', 'Package Name', 'Package ID', 'Event Data'])]

        # if not (pd.isnull(df_event_data_modernized['Event Data'].explode()).all() and pd.isnull(
        # df_event_data_legacy['Event Data'].explode()).all()):
        if not (pd.isnull(df_event_data_modernized['Event Data'].explode()).all()) and not (
                pd.isnull(df_event_data_legacy['Event Data'].explode()).all()):
            list_of_event_data_legacy = df_event_data_legacy['Event Data'].explode()
            new_df_event_data_legacy = pd.json_normalize(list_of_event_data_legacy)
            filtered_df_event_data_legacy = new_df_event_data_legacy[
                ~new_df_event_data_legacy['tagName'].isin(event_data_ignore_params)]
            filtered_df_event_data_legacy = filtered_df_event_data_legacy.drop(columns=event_datadf_ignore_columns)
            filtered_df_event_data_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_event_data_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_event_data_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            list_of_event_data_modernized = df_event_data_modernized['Event Data'].explode()
            new_df_event_data_modernized = pd.json_normalize(list_of_event_data_modernized)
            filtered_df_event_data_modernized = new_df_event_data_modernized[
                ~new_df_event_data_modernized['tagName'].isin(event_data_ignore_params)]
            filtered_df_event_data_modernized = filtered_df_event_data_modernized.drop(
                columns=event_datadf_ignore_columns)
            filtered_df_event_data_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_event_data_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_event_data_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_event_data_legacy.reset_index()
            df1['value'] = pd.to_numeric(df1['value'], errors='coerce')
            df2 = filtered_df_event_data_modernized.reset_index()
            df2['value'] = pd.to_numeric(df2['value'], errors='coerce')
            merged_df = pd.merge(df1, df2, how='outer', indicator=True)
            # diff_values = merged_df[merged_df['_merge']!='both']
            merged_df['Status'] = merged_df['_merge'].map(
                {'left_only': 'legacy', 'right_only': 'K8s', 'both': 'both'})

            rows_legacy = []
            rows_modernized = []
            rows_both = []

            for index, row in merged_df.iterrows():
                if row['Status'] == 'legacy':
                    rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                        row['siteTime'], "-", 'only found in legacy'])
                    rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp',
                                        row['timeStamp'], "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory',
                         row['eventCategory'], "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', row['value'], "-",
                         'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', row['tagName'],
                         "-", 'only found in legacy'])
                elif row['Status'] == 'K8s':
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime', "-",
                         row['siteTime'], 'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp', "-",
                         row['timeStamp'], 'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory', "-",
                         row['eventCategory'], 'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', "-", row['value'],
                         'only found in k8s'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', "-",
                         row['tagName'], 'only found in k8s'])
                else:
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                      row['siteTime'], row['siteTime'], 'output matched'])
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp',
                                      row['timeStamp'], row['timeStamp'], 'output matched'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory',
                         row['eventCategory'], row['eventCategory'], 'output matched'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', row['value'],
                         row['value'], 'output matched'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', row['tagName'],
                         row['tagName'], 'output matched'])

            comparison_df_legacy = pd.DataFrame(rows_legacy,
                                                columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                         'Parameter', 'Legacy', 'K8s', 'Status'])

            comparison_df_modernized = pd.DataFrame(rows_modernized,
                                                    columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                             'Parameter', 'Legacy', 'K8s', 'Status'])

            comparison_df_both = pd.DataFrame(rows_both,
                                              columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                       'Parameter', 'Legacy', 'K8s', 'Status'])

            result_df = pd.concat([comparison_df_legacy, comparison_df_modernized, comparison_df_both],
                                  ignore_index=True)

            if len(df1) != 0 or len(df2) != 0:
                result_df.fillna("-", inplace=True)
                filtered_df_valueEventData_both = result_df[result_df['Parameter'] == 'value']
                total_rows = filtered_df_valueEventData_both.shape[0]
                matches = filtered_df_valueEventData_both[
                    filtered_df_valueEventData_both['Legacy'] == filtered_df_valueEventData_both['K8s']].shape[0]
                percent_match = (matches / total_rows) * 100

                if len(df1) == 0:
                    count_match = (0 / len(df2)) * 100 if len(df2) != 0 else "N/A"
                    df_summary_event_data_both = pd.DataFrame(
                        {
                            'Product Line':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                    0],
                            'Package Name':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                    0],
                            'Package ID':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                    0],
                            'Legacy Event Datapoints': len(df1),
                            'K8s Event Datapoints': len(df2),
                            'Common Event Datapoints': total_rows,
                            'Value Match %': ["N/A"],
                            'Count Match %': [count_match]

                        })
                elif len(df2) == 0:
                    count_match = (0 / len(df1)) * 100 if len(df1) != 0 else "N/A"
                    df_summary_event_data_both = pd.DataFrame(
                        {
                            'Product Line':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                    0],
                            'Package Name':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                    0],
                            'Package ID':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                    0],
                            'Legacy Event Datapoints': len(df1),
                            'K8s Event Datapoints': len(df2),
                            'Common Event Datapoints': 0,
                            'Value Match %': ["N/A"],
                            'Count Match %': [count_match]

                        })
                else:
                    count_match = (len(df2) / len(df1)) * 100 if len(df1) != 0 else "N/A"
                    df_summary_event_data_both = pd.DataFrame(
                        {
                            'Product Line':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                    0],
                            'Package Name':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                    0],
                            'Package ID':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                    0],
                            'Legacy Event Datapoints': len(df1),
                            'K8s Event Datapoints': len(df2),
                            'Common Event Datapoints': total_rows,
                            'Value Match %': [percent_match],
                            'Count Match %': [count_match]
                        })

                df_summary_event_data_result_both = pd.concat(
                    [df_summary_event_data_result_both, df_summary_event_data_both], ignore_index=True)
            else:
                df_summary_event_data_both = pd.DataFrame(
                    {
                        'Product Line':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                0],
                        'Package Name':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                0],
                        'Package ID':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                0],
                        'Legacy Event Datapoints': 0,
                        'K8s Event Datapoints': 0,
                        'Common Event Datapoints': 0,
                        'Value Match %': ["N/A"],
                        'Count Match %': ["N/A"]
                    })

                df_summary_event_data_result_both = pd.concat(
                    [df_summary_event_data_result_both, df_summary_event_data_both], ignore_index=True)

            df_result_value_in_both = pd.concat([df_result_value_in_both, result_df], ignore_index=True)

        elif pd.isnull(df_event_data_modernized['Event Data'].explode()).all() and not pd.isnull(
                df_event_data_legacy['Event Data'].explode()).all():

            list_of_event_data_legacy = df_event_data_legacy['Event Data'].explode()
            new_df_event_data_legacy = pd.json_normalize(list_of_event_data_legacy)
            filtered_df_event_data_legacy = new_df_event_data_legacy[
                ~new_df_event_data_legacy['tagName'].isin(event_data_ignore_params)]
            filtered_df_event_data_legacy = filtered_df_event_data_legacy.drop(columns=event_datadf_ignore_columns)
            filtered_df_event_data_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_event_data_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_event_data_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_legacy = []
            df1 = filtered_df_event_data_legacy.reset_index()
            for index, row in filtered_df_event_data_legacy.iterrows():
                rows_legacy.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime', row['siteTime'],
                     "-", 'only found in legacy'])
                rows_legacy.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp', row['timeStamp'],
                     "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory',
                                    row['eventCategory'], "-", 'only found in legacy'])
                rows_legacy.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', row['value'], "-",
                     'only found in legacy'])
                rows_legacy.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', row['tagName'],
                     "-", 'only found in legacy'])

            result_df = pd.DataFrame(rows_legacy, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                           'Parameter', 'Legacy', 'K8s', 'Status'])

            if len(df1) != 0:
                count_match = (0 / len(df1)) * 100 if len(df1) != 0 else "N/A"
                result_df.fillna("-", inplace=True)
                # filtered_df_valueEventData_legacy = result_df[result_df['Parameter'] == 'value']
                # total_rows = filtered_df_valueEventData_legacy.shape[0]
                # matches = filtered_df_valueEventData_legacy[
                #     filtered_df_valueEventData_legacy['Legacy'] == filtered_df_valueEventData_legacy['K8s']].shape[0]
                # percent_match = (matches / total_rows) * 100
                df_summary_event_data_legacy = pd.DataFrame(
                    {
                        'Product Line':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                        'Package Name':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                        'Package ID':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                0],
                        'Legacy Event Datapoints': len(df1),
                        'K8s Event Datapoints': 0,
                        'Common Event Datapoints': 0,
                        'Value Match %': ["N/A"],
                        'Count Match %': [count_match]
                    })
                df_summary_event_data_result_legacy = pd.concat(
                    [df_summary_event_data_result_legacy, df_summary_event_data_legacy], ignore_index=True)

            elif len(df1) == 0:
                count_match = "N/A"
                df_summary_event_data_legacy = pd.DataFrame(
                    {
                        'Product Line':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                0],
                        'Package Name':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                0],
                        'Package ID':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                0],
                        'Legacy Event Datapoints': 0,
                        'K8s Event Datapoints': 0,
                        'Common Event Datapoints': 0,
                        'Value Match %': ["N/A"],
                        'Count Match %': [count_match]

                    })
                df_summary_event_data_result_legacy = pd.concat(
                    [df_summary_event_data_result_legacy, df_summary_event_data_legacy], ignore_index=True)

            df_result_value_in_legacy = pd.concat([df_result_value_in_legacy, result_df], ignore_index=True)

        elif pd.isnull(df_event_data_legacy['Event Data'].explode()).all() and not pd.isnull(
                df_event_data_modernized['Event Data'].explode()).all():
            list_of_eventData_modernized = df_event_data_modernized['Event Data'].explode()
            new_df_event_data_modernized = pd.json_normalize(list_of_eventData_modernized)
            filtered_df_event_data_modernized = new_df_event_data_modernized[
                ~new_df_event_data_modernized['tagName'].isin(event_data_ignore_params)]
            filtered_df_event_data_modernized = filtered_df_event_data_modernized.drop(
                columns=event_datadf_ignore_columns)
            filtered_df_event_data_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_event_data_modernized.insert(1, 'Package Name', df_legacy.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_event_data_modernized.insert(2, 'Package ID', df_legacy.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_event_data_modernized.reset_index()
            rows_modernized = []
            for index, row in filtered_df_event_data_modernized.iterrows():
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime', "-",
                     row['siteTime'], 'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp', "-",
                     row['timeStamp'], 'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory', "-",
                     row['eventCategory'], 'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', "-", row['value'],
                     'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', "-", row['tagName'],
                     'only found in k8s'])

            result_df = pd.DataFrame(rows_modernized, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                               'Parameter', 'Legacy', 'K8s', 'Status'])
            if len(df1) != 0:
                result_df.fillna("-", inplace=True)
                # filtered_df_valueEventData_legacy = result_df[result_df['Parameter'] == 'value']
                # total_rows = filtered_df_valueEventData_legacy.shape[0]
                # matches = filtered_df_valueEventData_legacy[
                #     filtered_df_valueEventData_legacy['Legacy'] == filtered_df_valueEventData_legacy['K8s']].shape[0]
                # percent_match = (matches / total_rows) * 100
                count_match = (0 / len(df1)) * 100 if len(df1) != 0 else "N/A"
                df_summary_event_data_modernized = pd.DataFrame(
                    {
                        'Product Line':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                        'Package Name':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                        'Package ID':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                0],
                        'Legacy Event Datapoints': 0,
                        'K8s Event Datapoints': len(df1),
                        'Common Event Datapoints': 0,
                        'Value Match %': ["N/A"],
                        'Count Match %': [count_match]
                    })
                df_summary_event_data_result_modernized = pd.concat(
                    [df_summary_event_data_result_modernized, df_summary_event_data_modernized], ignore_index=True)
            df_result_value_in_modernized = pd.concat([df_result_value_in_modernized, result_df], ignore_index=True)

        else:
            product_line = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Product Line'].iloc[0]
            package_name = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Package Name'].iloc[0]
            package_id = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Package ID'].iloc[0]
            rows = []
            rows.append([product_line, package_name, package_id, "-", 'siteTime', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'timeStamp', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'eventCategory', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'value', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", "tagName", "-", "-",
                         'No data found in both Legacy and K8s'])
            df_result = pd.DataFrame(rows, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                    'Parameter', 'Legacy', 'K8s', 'Status'])

            df_summary_agent_msg_data = pd.DataFrame({
                'Product Line': product_line,
                'Package Name': package_name,
                'Package ID': package_id,
                'Legacy Event Datapoints': 0,
                'K8s Event Datapoints': 0,
                'Common Event Datapoints': 0,
                'Value Match %': ["N/A"],
                'Count Match %': ["N/A"]
            })
            df_result_value_null = pd.concat([df_result_value_null, df_result], ignore_index=False)
            df_summary_event_data_result_null = pd.concat(
                [df_summary_event_data_result_null, df_summary_agent_msg_data],
                ignore_index=False)

    event_data_result_df = pd.concat([df_result_value_in_both, df_result_value_in_legacy, df_result_value_in_modernized,
                                      df_result_value_null])
    # event_data_result_df.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    event_data_result_df.reset_index(drop=True, inplace=True)
    summary_table_event_data = pd.concat([df_summary_event_data_result_legacy, df_summary_event_data_result_modernized,
                                          df_summary_event_data_result_both, df_summary_event_data_result_null],
                                         ignore_index=True)
    # summary_table_event_data.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    # eventData_csv_file = r'output/EventData_output.csv'
    # event_data_result_df.to_csv(eventData_csv_file, index=True)
    return event_data_result_df, summary_table_event_data


# function to compare Agent Messages from Legacy vs Modernized DMA json file
def compare_agent_messages(df_legacy, df_modernized, df_package_id_legacy, df_package_id_modernized):
    """
    :param df_legacy: str
    :param df_modernized: str
    :param df_package_id_legacy: str
    :param df_package_id_modernized: str
    :return: csv_filename: str
    """
    package_id_list = np.union1d(df_package_id_legacy, df_package_id_modernized)

    agent_message_ignore_params = ["creationTime", "hasSpecificText", "agentId", "assemblyId", "categoryId",
                                   "fileIdentifier", "agentSpecifics", "messageTextId",
                                   "agentMessageId", "limitValue", "agentSpecCat", "tagId", "messageMapId"]
    ignore_message_texts = ['Task chain for',
                            'Task closed manually',
                            'Task opened manually']

    df_result_value_in_both = pd.DataFrame()
    df_result_value_in_legacy = pd.DataFrame()
    df_result_value_in_modernized = pd.DataFrame()
    df_result_value_null = pd.DataFrame()
    df_summary_agent_msgdata_result_both = pd.DataFrame()
    df_summary_agent_msgdata_result_legacy = pd.DataFrame()
    df_summary_agent_msgdata_result_modernized = pd.DataFrame()
    df_summary_agent_msgdata_result_null = pd.DataFrame()
    for eachPackageID in package_id_list:

        df_agent_messagedata_legacy = df_legacy.loc[(df_legacy["Package ID"] == eachPackageID,
                                                     ['Product Line', 'Package Name', 'Package ID', 'Agent Messages'])]

        df_agent_messagedata_modernized = df_modernized.loc[(df_modernized["Package ID"] == eachPackageID,
                                                             ['Product Line', 'Package Name', 'Package ID',
                                                              'Agent Messages'])]

        if not pd.isnull(df_agent_messagedata_modernized['Agent Messages'].explode()).all() and not pd.isnull(
                df_agent_messagedata_legacy['Agent Messages'].explode()).all():

            list_of_agent_message_legacy = df_agent_messagedata_legacy['Agent Messages'].explode()
            new_df_agent_message_legacy = pd.json_normalize(list_of_agent_message_legacy)
            regex_pattern = '|'.join(ignore_message_texts)

            # for messageText in ignore_message_texts:
            #     print(messageText)
            #     new_df_agent_message_legacy = new_df_agent_message_legacy[
            #         ~new_df_agent_message_legacy['messageText'].str.contains(messageText, regex=True)]
            new_df_agent_message_legacy = new_df_agent_message_legacy[~new_df_agent_message_legacy['messageText'].str.contains(regex_pattern, regex=True)]

            if not new_df_agent_message_legacy.empty:
                filtered_df_agent_message_legacy = new_df_agent_message_legacy.drop(columns=agent_message_ignore_params)
            else:
                filtered_df_agent_message_legacy = pd.DataFrame()

            filtered_df_agent_message_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agent_message_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agent_message_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            list_of_agent_message_modernized = df_agent_messagedata_modernized['Agent Messages'].explode()
            new_df_agent_message_modernized = pd.json_normalize(list_of_agent_message_modernized)
            # for messageText in ignore_message_texts:
            #     new_df_agent_message_modernized = new_df_agent_message_modernized[
            #         ~new_df_agent_message_modernized['messageText'].str.contains(messageText, regex=True)]
            new_df_agent_message_modernized = new_df_agent_message_modernized[
                        ~new_df_agent_message_modernized['messageText'].str.contains(regex_pattern, regex=True)]

            if not new_df_agent_message_legacy.empty:
                filtered_df_agent_message_modernized = new_df_agent_message_modernized.drop(
                    columns=agent_message_ignore_params)
            else:
                filtered_df_agent_message_modernized = pd.DataFrame()

            filtered_df_agent_message_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agent_message_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agent_message_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_agent_message_legacy.reset_index()

            df2 = filtered_df_agent_message_modernized.reset_index()

            if df1.empty and df2.empty:
                comparison_df_both_null = pd.DataFrame(columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                                'Parameter', 'Legacy', 'K8s', 'Status'])
                agentmessage_result_df_both = comparison_df_both_null
            else:
                merged_df = pd.merge(df1, df2, on=['Product Line', 'Package Name', 'Package ID','siteEventTime','messageText','messageClass','messageSeverity','messageScope'],
                                     how='outer', indicator=True)
                # diff_values = merged_df[merged_df['_merge']!='both']
                merged_df['Status'] = merged_df['_merge'].map(
                    {'left_only': 'legacy', 'right_only': 'K8s', 'both': 'both'})

                rows_legacy = []
                rows_modernized = []
                rows_both = []

                for index, row in merged_df.iterrows():
                    if row['Status'] == 'legacy':
                        rows_legacy.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime',
                             row['siteEventTime'], "-", 'only found in legacy'])
                        rows_legacy.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText',
                             row['messageText'], "-", 'only found in legacy'])
                        rows_legacy.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass',
                             row['messageClass'], "-", 'only found in legacy'])
                        rows_legacy.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity',
                             row['messageSeverity'], "-", 'only found in legacy'])
                        rows_legacy.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope',
                             row['messageScope'], "-", 'only found in legacy'])
                    elif row['Status'] == 'K8s':
                        rows_modernized.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime', "-",
                             row['siteEventTime'], 'only found in k8s'])
                        rows_modernized.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText', "-",
                             row['messageText'], 'only found in k8s'])
                        rows_modernized.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass', "-",
                             row['messageClass'], 'only found in k8s'])
                        rows_modernized.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity', "-",
                             row['messageSeverity'], 'only found in k8s'])
                        rows_modernized.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope', "-",
                             row['messageScope'], 'only found in k8s'])
                    else:
                        rows_both.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime',
                             row['siteEventTime'], row['siteEventTime'], 'output matched'])
                        rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText',
                                          row['messageText'], row['messageText'], 'output matched'])
                        rows_both.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass',
                             row['messageClass'], row['messageClass'], 'output matched'])
                        rows_both.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity',
                             row['messageSeverity'], row['messageSeverity'], 'output matched'])
                        rows_both.append(
                            [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope',
                             row['messageScope'], row['messageScope'], 'output matched'])

                comparison_df_legacy = pd.DataFrame(rows_legacy,
                                                    columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                             'Parameter', 'Legacy', 'K8s', 'Status'])
                comparison_df_modernized = pd.DataFrame(rows_modernized,
                                                        columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                                 'Parameter', 'Legacy', 'K8s', 'Status'])
                comparison_df_both = pd.DataFrame(rows_both,
                                                  columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                           'Parameter', 'Legacy', 'K8s', 'Status'])


                agentmessage_result_df_both = pd.concat(
                    [comparison_df_legacy, comparison_df_modernized, comparison_df_both],ignore_index=True)

            if len(df1) != 0 or len(df2) != 0:
                agentmessage_result_df_both.fillna("-", inplace=True)
                filtered_df_msgtext_agent_msgdata_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageText']
                filtered_df_msgclass_agent_msgdata_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageClass']
                filtered_df_msgseverity_agent_msgdata_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageSeverity']
                filtered_df_msgscope_agentmsgdata_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageScope']

                total_rows_msgtext = filtered_df_msgtext_agent_msgdata_both.shape[0]
                total_rows_msgclass = filtered_df_msgclass_agent_msgdata_both.shape[0]
                total_rows_msgseverity = filtered_df_msgseverity_agent_msgdata_both.shape[0]
                total_rows_msgscope = filtered_df_msgscope_agentmsgdata_both.shape[0]

                matches_msgtext = filtered_df_msgtext_agent_msgdata_both[
                    filtered_df_msgtext_agent_msgdata_both['Legacy'] == filtered_df_msgtext_agent_msgdata_both[
                        'K8s']].shape[0]
                matches_msgclass = filtered_df_msgclass_agent_msgdata_both[
                    filtered_df_msgclass_agent_msgdata_both['Legacy'] == filtered_df_msgclass_agent_msgdata_both[
                        'K8s']].shape[0]
                matches_msgseverity = filtered_df_msgseverity_agent_msgdata_both[
                    filtered_df_msgseverity_agent_msgdata_both['Legacy'] == filtered_df_msgseverity_agent_msgdata_both[
                        'K8s']].shape[0]
                matches_msgscope = filtered_df_msgscope_agentmsgdata_both[
                    filtered_df_msgscope_agentmsgdata_both['Legacy'] == filtered_df_msgscope_agentmsgdata_both[
                        'K8s']].shape[0]

                percent_match_msgtext = (matches_msgtext / total_rows_msgtext) * 100

                percent_match_msgclass = (matches_msgclass / total_rows_msgclass) * 100
                percent_match_msgseverity = (matches_msgseverity / total_rows_msgseverity) * 100
                percent_match_msgscope = (matches_msgscope / total_rows_msgscope) * 100
                if len(df1) == 0:
                    df_summary_agent_msg_data_both = pd.DataFrame(
                        {
                            'Product Line':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                    0],
                            'Package Name':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                    0],
                            'Package ID':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                    0],
                            'Legacy Agent Message Datapoints': len(df1),
                            'Modernized Agent Message Datapoints': len(df2),
                            # 'Common Agent Message Datapoints': 0,
                            "msgText Match%": [percent_match_msgtext],
                            "msgClass Match%": [percent_match_msgclass],
                            "msgSeverity Match%": [percent_match_msgseverity],
                            "msgScope Match%": [percent_match_msgscope]
                        })
                elif len(df2) == 0:
                    df_summary_agent_msg_data_both = pd.DataFrame(
                        {
                            'Product Line':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                    0],
                            'Package Name':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                    0],
                            'Package ID':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                    0],
                            'Legacy Agent Message Datapoints': len(df1),
                            'Modernized Agent Message Datapoints': len(df2),
                            # 'Common Agent Message Datapoints': 0,
                            "msgText Match%": [percent_match_msgtext],
                            "msgClass Match%": [percent_match_msgclass],
                            "msgSeverity Match%": [percent_match_msgseverity],
                            "msgScope Match%": [percent_match_msgscope]
                        })
                else:
                    df_summary_agent_msg_data_both = pd.DataFrame(
                        {
                            'Product Line':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                    0],
                            'Package Name':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                    0],
                            'Package ID':
                                df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                    0],
                            'Legacy Agent Message Datapoints': len(df1),
                            'Modernized Agent Message Datapoints': len(df2),
                            # 'Common Agent Message Datapoints': 0,
                            "msgText Match%": [percent_match_msgtext],
                            "msgClass Match%": [percent_match_msgclass],
                            "msgSeverity Match%": [percent_match_msgseverity],
                            "msgScope Match%": [percent_match_msgscope]
                        })
            else:
                df_summary_agent_msg_data_both = pd.DataFrame(
                    {
                        'Product Line':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                                0],
                        'Package Name':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                                0],
                        'Package ID':
                            df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                                0],
                        'Legacy Agent Message Datapoints': len(df1),
                        'Modernized Agent Message Datapoints': len(df2),
                        # 'Common Agent Message Datapoints': 0,
                        "msgText Match%": ["N/A"],
                        "msgClass Match%": ["N/A"],
                        "msgSeverity Match%": ["N/A"],
                        "msgScope Match%": ["N/A"]
                    })


            df_summary_agent_msgdata_result_both = pd.concat(
                [df_summary_agent_msgdata_result_both, df_summary_agent_msg_data_both], ignore_index=True)
            df_result_value_in_both = pd.concat([df_result_value_in_both, agentmessage_result_df_both],
                                                ignore_index=True)

        elif pd.isnull(df_agent_messagedata_modernized['Agent Messages'].explode()).all() and not pd.isnull(
                df_agent_messagedata_legacy['Agent Messages'].explode()).all():
            list_of_agent_message_legacy = df_agent_messagedata_legacy['Agent Messages'].explode()
            new_df_agent_message_legacy = pd.json_normalize(list_of_agent_message_legacy)
            for messageText in ignore_message_texts:
                new_df_agent_message_legacy = new_df_agent_message_legacy[
                    ~new_df_agent_message_legacy['messageText'].str.contains(messageText, regex=True)]
            filtered_df_agent_message_legacy = new_df_agent_message_legacy.drop(columns=agent_message_ignore_params)

            filtered_df_agent_message_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agent_message_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agent_message_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_legacy = []
            for index, row in filtered_df_agent_message_legacy.iterrows():
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime',
                                    row['siteEventTime'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText',
                                    row['messageText'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass',
                                    row['messageClass'], "-", 'only found in legacy'])
                rows_legacy.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity',
                     row['messageSeverity'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope',
                                    row['messageScope'], "-", 'only found in legacy'])

            result_df_legacy = pd.DataFrame(rows_legacy, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                                  'Parameter', 'Legacy', 'K8s', 'Status'])

            result_df_legacy.fillna("-", inplace=True)
            df_summary_agentMsg_data_legacy = pd.DataFrame(
                {
                    'Product Line':
                        df_legacy.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                            0],
                    'Package Name':
                        df_legacy.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                            0],
                    'Package ID':
                        df_legacy.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                            0],
                    'Legacy Agent Message Datapoints': len(filtered_df_agent_message_legacy),
                    'Modernized Agent Message Datapoints': 0,
                    "msgText Match%": ["N/A"],
                    "msgClass Match%": ["N/A"],
                    "msgSeverity Match%": ["N/A"],
                    "msgScope Match%": ["N/A"]
                })

            df_summary_agent_msgdata_result_legacy = pd.concat(
                [df_summary_agent_msgdata_result_legacy, df_summary_agentMsg_data_legacy], ignore_index=True)

            df_result_value_in_legacy = pd.concat([df_result_value_in_legacy, result_df_legacy], ignore_index=True)

        elif (not pd.isnull(df_agent_messagedata_modernized['Agent Messages'].explode()).all() and
              pd.isnull(df_agent_messagedata_legacy['Agent Messages'].explode()).all()):
            list_of_agent_message_modernized = df_agent_messagedata_modernized['Agent Messages'].explode()
            new_df_agent_message_modernized = pd.json_normalize(list_of_agent_message_modernized)
            filtered_df_agent_message_modernized = new_df_agent_message_modernized.drop(
                columns=agent_message_ignore_params)
            filtered_df_agent_message_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agent_message_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agent_message_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_modernized = []

            for index, row in filtered_df_agent_message_modernized.iterrows():
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime', "-",
                     row['siteEventTime'], 'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText', "-",
                     row['messageText'], 'only found in k8s'])

                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass', "-",
                     row['messageClass'], 'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity', "-",
                     row['messageSeverity'], 'only found in k8s'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope', "-",
                     row['messageScope'], 'only found in k8s'])

            result_df_modernized = pd.DataFrame(rows_modernized,
                                                columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                         'Parameter', 'Legacy', 'K8s', 'Status'])

            result_df_modernized.fillna("-", inplace=True)
            df_summary_agent_msg_data_modernized = pd.DataFrame(
                {
                    'Product Line':
                        df_legacy.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[
                            0],
                    'Package Name':
                        df_legacy.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[
                            0],
                    'Package ID':
                        df_legacy.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                            0],
                    'Legacy Agent Message Datapoints': 0,
                    'Modernized Agent Message Datapoints': len(filtered_df_agent_message_modernized),
                    "msgText Match%": ["N/A"],
                    "msgClass Match%": ["N/A"],
                    "msgSeverity Match%": ["N/A"],
                    "msgScope Match%": ["N/A"]
                })

            df_summary_agent_msgdata_result_modernized = pd.concat(
                [df_summary_agent_msgdata_result_modernized, df_summary_agent_msg_data_modernized], ignore_index=True)

            df_result_value_in_modernized = pd.concat([df_result_value_in_modernized, result_df_modernized],
                                                      ignore_index=False)

        else:
            product_line = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Product Line'].iloc[0]
            package_name = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Package Name'].iloc[0]
            package_id = df_legacy.loc[df_legacy['Package ID'] == eachPackageID, 'Package ID'].iloc[0]
            rows = []
            rows.append([product_line, package_name, package_id, "-", 'siteEventTime', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'messageText', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'messageClass', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'messageSeverity', "-", "-",
                         'No data found in both Legacy and K8s'])
            rows.append([product_line, package_name, package_id, "-", 'messageScope', "-", "-",
                         'No data found in both Legacy and K8s'])
            df_result = pd.DataFrame(rows, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                    'Parameter', 'Legacy', 'K8s', 'Status'])

            df_summary_agent_msg_data = pd.DataFrame({
                'Product Line': product_line,
                'Package Name': package_name,
                'Package ID': package_id,
                'Legacy Agent Message Datapoints': 0,
                'Modernized Agent Message Datapoints': 0,
                "msgText Match%": ["N/A"],
                "msgClass Match%": ["N/A"],
                "msgSeverity Match%": ["N/A"],
                "msgScope Match%": ["N/A"]
            })
            df_result_value_null = pd.concat([df_result_value_null, df_result], ignore_index=False)
            df_summary_agent_msgdata_result_null = pd.concat(
                [df_summary_agent_msgdata_result_null, df_summary_agent_msg_data],
                ignore_index=False)

    agent_messagedata_result_df = pd.concat(
        [df_result_value_in_both, df_result_value_in_legacy, df_result_value_in_modernized, df_result_value_null])
    # eventlData_result_df = eventlData_result_df.sort_values(by='Product Line')
    agent_messagedata_result_df.reset_index(drop=True, inplace=True)
    summary_table_agent_messagedata = pd.concat(
        [df_summary_agent_msgdata_result_legacy, df_summary_agent_msgdata_result_modernized,
         df_summary_agent_msgdata_result_both, df_summary_agent_msgdata_result_null], ignore_index=True)

    summary_table_agent_messagedata.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True],
                                                inplace=True)
    # agentMessageData_csv_file = r'output/AgentMessageData_output.csv'
    # agent_messagedata_result_df.to_csv(agentMessageData_csv_file, index=True)
    return agent_messagedata_result_df, summary_table_agent_messagedata


def compare_output(script_dir, s3_bucket_name, s3_prefix, input_folder_path, timestamp):
    # Input Data
    # Download the input json files Legacy and Modernized from S3 to local
    # download_json_from_s3(s3_bucket_name, s3_prefix, input_folder_path)
    input_folder_path = os.path.join(script_dir, 'input_json')
    # legacy_data_json = f'{input_folder_path}/DMA_Legacy.json'
    legacy_data_json = f'{input_folder_path}/test_DMA_Legacy.json'
    # modernized_data_json = f'{input_folder_path}/DMA_Modernized.json'
    modernized_data_json = f'{input_folder_path}/test_DMA_Modernized.json'
    df_legacy = pd.read_json(legacy_data_json)
    df_modernized = pd.read_json(modernized_data_json)
    df_productline_legacy = df_legacy['Product Line'].unique()
    df_productline_modernized = df_modernized['Product Line'].unique()
    df_package_id_legacy = df_legacy['Package ID'].unique()
    df_package_id_modernized = df_modernized['Package ID'].unique()

    # Comparison OutputDataFrames
    log_info(f'Executing Legacy vs Modernized output comparison script', timestamp)
    try:
        df1 = compare_product_lines(df_productline_legacy, df_productline_modernized)
        log_success(f"Comparing Product Lines completed.", timestamp)
        df2 = compare_package_names_for_productline(df_legacy, df_modernized, df_productline_legacy,
                                                    df_productline_modernized)
        log_success(f"Comparing Package Names completed.", timestamp)
        df3a_data, df3b_summary = compare_signal_data(df_legacy, df_modernized, df_package_id_legacy,
                                                      df_package_id_modernized)
        log_success("Comparing Signal Data completed.", timestamp)
        df4a_data, df4b_summary = compare_event_data(df_legacy, df_modernized, df_package_id_legacy,
                                                     df_package_id_modernized)
        log_success("Comparing Event Data completed.", timestamp)
        df5a_data, df5b_summary = compare_agent_messages(df_legacy, df_modernized, df_package_id_legacy,
                                                         df_package_id_modernized)
        log_success("Comparing Agent Messages completed.", timestamp)

        output_folder_path = os.path.join(script_dir, 'output_csv')
        output_file = os.path.join(output_folder_path, 'output_comparison_data.xlsx')

        log_success('Saving output to a file..', timestamp)
        with pd.ExcelWriter(output_file) as writer:
            df1.to_excel(writer, sheet_name='ProductLine_output', index=False)
            df2.to_excel(writer, sheet_name='PackageNames_output', index=False)
            df3b_summary.to_excel(writer, sheet_name='SignalData_summary', index=False)
            df4b_summary.to_excel(writer, sheet_name='EventData_summary', index=False)
            df5b_summary.to_excel(writer, sheet_name='AgentMessageData_summary', index=False)
            df3a_data.to_excel(writer, sheet_name='SignalData_output', index=False)
            df4a_data.to_excel(writer, sheet_name='EventData_output', index=False)
            df5a_data.to_excel(writer, sheet_name='AgentMessageData_output', index=False)

        log_success('Comparison report has been generated..', timestamp)
        # Upload output file to s3
        # log_success("Uploading output comparison file to AWS S3..", timestamp)
        # filename = os.path.basename(output_file)
        # upload_comparison_output_file_to_s3(s3_bucket_name, filename, output_file, timestamp)
    except Exception as e:
        log_error(e, timestamp)
