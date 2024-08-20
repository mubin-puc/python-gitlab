import pandas as pd
import numpy as np
import os
#import boto3
#from datetime import datetime
from src.common_utils import download_json_from_s3, upload_comparison_output_file_to_s3


# function to compare Product Lines from Legacy vs Modernized DMA json file
def compare_product_lines(df_productLine_legacy, df_productLine_modernized):
    """
    :param df_productLine_legacy: str
    :param df_productLine_modernized: str
    :return: csv_filename: str
    """
    missing_in_legacy = np.setdiff1d(df_productLine_modernized, df_productLine_legacy)
    missing_in_modernized = np.setdiff1d(df_productLine_legacy, df_productLine_modernized)
    if missing_in_legacy.size > 0 or missing_in_modernized.size > 0:
        df_missing_productLines_legacy = pd.DataFrame({
            "Type": "DMA_Legacy",
            "ProductLine": missing_in_legacy,
            "Status": "Not Found"
        })
        df_missing_productLines_modernized = pd.DataFrame({
            "Type": "DMA_Modernized",
            "ProductLine": missing_in_modernized,
            "Status": "Not Found"
        })

        df_pl_difference = pd.concat([df_missing_productLines_legacy, df_missing_productLines_modernized])
        # productLine_csv_file = r'output/Productline_output.csv'

        # df_pl_difference.to_csv(productLine_csv_file, index=False)
        return df_pl_difference
    else:

        df_pl = pd.DataFrame(
            {'Legacy_ProductLines': df_productLine_legacy, 'Modernized_ProductLines': df_productLine_modernized,
             "Status": "Product Lines found both in Legacy and Modernized"})
        # header_text = 'No missing Product Lines in both Legacy and Modernized json files.'
        # df_pl.columns = pd.MultiIndex.from_tuples(
        #     zip([header_text, '',''], df_pl.columns)
        # )
        # df_pl.reset_index(inplace=True)
        # productLine_csv_file = r'output/Productline_output.csv'
        # df_pl.to_csv(productLine_csv_file, index=False)
        return df_pl


# function to compare Package Names from Legacy vs Modernized DMA json file
def compare_packageNames_for_productLine(df_legacy, df_modernized, df_productLine_legacy, df_productLine_modernized):
    """
    :param df_productLine_legacy: str
    :param df_productLine_modernized: str
    :return: csv_filename
    """
    productLines_to_compare = np.union1d(df_productLine_modernized, df_productLine_legacy)
    # pl stands for product line
    # pi stands for packageid
    df_pl_pi_difference = pd.DataFrame()
    df_pl_pi_similar = pd.DataFrame()
    for eachProductLine in productLines_to_compare:
        df_packageNames_legacy = df_legacy.loc[
            (df_legacy["Product Line"] == eachProductLine, ['Product Line', 'Package Name', 'Package ID'])]
        df_packageNames_modernized = df_modernized.loc[
            (df_modernized["Product Line"] == eachProductLine, ['Product Line', 'Package Name', 'Package ID'])]

        are_equal = df_packageNames_legacy.equals(df_packageNames_modernized)
        if not are_equal:
            df_unique_packageNames_legacy = df_packageNames_legacy["Package Name"].unique()
            df_unique_packageIDs_legacy = df_packageNames_legacy["Package ID"].unique()
            df_unique_packageNames_modernized = df_packageNames_modernized["Package Name"].unique()
            df_unique_packageIDs_modernized = df_packageNames_modernized["Package ID"].unique()
            missing_packageName_in_legacy = np.setdiff1d(df_unique_packageNames_modernized,
                                                         df_unique_packageNames_legacy)
            missing_packageID_in_legacy = np.setdiff1d(df_unique_packageIDs_modernized, df_unique_packageIDs_legacy)
            missing_packageName_in_modernized = np.setdiff1d(df_unique_packageNames_legacy,
                                                             df_unique_packageNames_modernized)
            missing_packageID_in_modernized = np.setdiff1d(df_unique_packageIDs_legacy, df_unique_packageIDs_modernized)
            if missing_packageName_in_legacy.size > 0 or missing_packageName_in_modernized.size > 0:
                df_missing_packageName_legacy = pd.DataFrame({
                    "Type": "DMA_Legacy",
                    "ProductLine": eachProductLine,
                    "PackageName": missing_packageName_in_legacy,
                    "PackageID": missing_packageID_in_legacy,
                    "Status": "Not Found in DMA Legacy"
                })

                df_missing_packageID_modernized = pd.DataFrame({
                    "Type": "DMA_Modernized",
                    "ProductLine": eachProductLine,
                    "PackageName": missing_packageName_in_modernized,
                    "PackageID": missing_packageID_in_modernized,
                    "Status": "Not Found in DMA Modernized"
                })

                df_packagenames = pd.concat([df_missing_packageName_legacy, df_missing_packageID_modernized])
                df_pl_pi_difference = pd.concat([df_packagenames, df_pl_pi_difference])


        else:

            # merged_df = pd.merge(df_packageNames_legacy,df_packageNames_modernized, on='Product Line', how='outer')
            # merged_df['Status']='Found in both legacy and modernized'
            # df_pl_pi_similar = pd.concat([merged_df,df_pl_pi_similar])
            df_packageNames_legacy = df_legacy.loc[
                (df_legacy["Product Line"] == eachProductLine, 'Package Name')]
            df_packageID_legacy = df_legacy.loc[(df_legacy["Product Line"] == eachProductLine, 'Package ID')]
            df_packageNames_modernized = df_modernized.loc[
                (df_modernized["Product Line"] == eachProductLine, 'Package Name')]
            df_packageID_modernized = df_modernized.loc[
                (df_modernized["Product Line"] == eachProductLine, 'Package ID')]

            df_pl_pi = pd.DataFrame({
                "ProductLine": eachProductLine,
                "PackageName_legacy": df_packageNames_legacy,
                "PackageID_legacy": df_packageID_legacy,
                "PackageName_modernized": df_packageNames_modernized,
                "PackageID_modernized": df_packageID_modernized,
                "Status": "Package found in both Legacy and Modenized"
            })
            df_pl_pi_similar = pd.concat([df_pl_pi, df_pl_pi_similar])

    if not df_pl_pi_difference.empty:
        df_pl_pi_difference = df_pl_pi_difference.sort_values('ProductLine')
        return df_pl_pi_difference
    else:
        df_pl_pi_similar.sort_values(by='ProductLine', inplace=True)
        return df_pl_pi_similar


# function to compare Signal Data from Legacy vs Modernized DMA json file
def compare_signal_data(df_legacy, df_modernized, df_packageID_legacy, df_packageID_modernized):
    """

    :param df_packageID_legacy: str
    :param df_packageID_modernized: str
    :return: csv_filename: str
    """
    package_id_list = np.union1d(df_packageID_legacy, df_packageID_modernized)
    signalData_params = ['siteTime', 'tagName', 'tagAlias', 'value']
    df_result_value_in_both = pd.DataFrame()
    df_result_value_in_legacy = pd.DataFrame()
    df_result_value_in_modernized = pd.DataFrame()
    df_summary_signalData_result_both = pd.DataFrame()
    df_summary_signalData_result_legacy = pd.DataFrame()
    df_summary_signalData_result_modernized = pd.DataFrame()
    for eachPackageID in package_id_list:
        df_signalData_legacy = df_legacy.loc[(df_legacy["Package ID"] == eachPackageID,
                                              ['Product Line', 'Package Name', 'Package ID', 'Signal Data'])]
        df_signalData_modernized = df_modernized.loc[(
        df_modernized["Package ID"] == eachPackageID, ['Product Line', 'Package Name', 'Package ID', 'Signal Data'])]

        if not pd.isnull(df_signalData_modernized['Signal Data'].explode()).all() and not pd.isnull(
                df_signalData_legacy['Signal Data'].explode()).all():

            list_of_signalData_legacy = df_signalData_legacy['Signal Data'].explode()
            new_df_signalData_legacy = pd.json_normalize(list_of_signalData_legacy)
            filtered_df_signalData_legacy = new_df_signalData_legacy[signalData_params]
            filtered_df_signalData_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signalData_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signalData_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            list_of_signalData_modernized = df_signalData_modernized['Signal Data'].explode()
            new_df_signalData_modernized = pd.json_normalize(list_of_signalData_modernized)
            filtered_df_signalData_modernized = new_df_signalData_modernized[signalData_params]
            filtered_df_signalData_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signalData_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signalData_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_signalData_legacy.reset_index()

            df1['value'] = pd.to_numeric(df1['value'], errors='coerce')
            df2 = filtered_df_signalData_modernized.reset_index()

            df2['value'] = pd.to_numeric(df2['value'], errors='coerce')
            merged_df = pd.merge(df1, df2, how='outer', indicator=True)

            # diff_values = merged_df[merged_df['_merge']!='both']
            merged_df['Status'] = merged_df['_merge'].map(
                {'left_only': 'legacy', 'right_only': 'modernized', 'both': 'both'})

            merged_df.index = merged_df.index + 1

            rows_legacy = []
            rows_modernized = []
            rows_both = []

            for index, row in merged_df.iterrows():
                if row['Status'] == 'legacy':
                    rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                        row['siteTime'], "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', row['tagName'],
                         "-", 'only found in legacy'])
                    rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias',
                                        row['tagAlias'], "-", 'only found in legacy'])
                    rows_legacy.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', row['value'], "-",
                         'only found in legacy'])
                elif row['Status'] == 'modernized':
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime', "-",
                         row['siteTime'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', "-",
                         row['tagName'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias', "-",
                         row['tagAlias'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', "-", row['value'],
                         'only found in modernized'])
                else:
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                      row['siteTime'], row['siteTime'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', row['tagName'],
                         row['tagName'], 'found in both'])
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias',
                                      row['tagAlias'], row['tagAlias'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', row['value'],
                         row['value'], 'found in both'])

            comparison_df_legacy = pd.DataFrame(rows_legacy,
                                                columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                         'Parameter', 'Legacy', 'Modernized', 'Status'])
            comparison_df_modernized = pd.DataFrame(rows_modernized,
                                                    columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                             'Parameter', 'Legacy', 'Modernized', 'Status'])
            comparison_df_both = pd.DataFrame(rows_both,
                                              columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                       'Parameter', 'Legacy', 'Modernized', 'Status'])

            result_df = pd.concat([comparison_df_legacy, comparison_df_modernized, comparison_df_both],
                                  ignore_index=True)

            result_df.fillna("-", inplace=True)
            filtered_df_valueSignalData_both = result_df[result_df['Parameter'] == 'value']
            total_rows = filtered_df_valueSignalData_both.shape[0]
            matches = filtered_df_valueSignalData_both[
                filtered_df_valueSignalData_both['Legacy'] == filtered_df_valueSignalData_both['Modernized']].shape[0]
            percent_match = (matches / total_rows) * 100
            count_match = (len(df2)/len(df1))*100

            df_summary_signal_data_both = pd.DataFrame(
                {
                    'Product Line':
                        df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                    'Package Name':
                        df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                    'Package ID': df_modernized.loc[(df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[
                        0],
                    'Legacy Signal Datapoints': len(df1),
                    'Modernized Signal Datapoints': len(df2),
                    'Value Match %': [percent_match],
                    'Count Match %': [count_match]
                }
            )
            df_summary_signalData_result_both = pd.concat(
                [df_summary_signalData_result_both, df_summary_signal_data_both], ignore_index=True)
            df_result_value_in_both = pd.concat([df_result_value_in_both, result_df], ignore_index=True)

        elif pd.isnull(df_signalData_modernized['Signal Data'].explode()).all() and not pd.isnull(
                df_signalData_legacy['Signal Data'].explode()).all():
            list_of_signalData_legacy = df_signalData_legacy['Signal Data'].explode()
            new_df_signalData_legacy = pd.json_normalize(list_of_signalData_legacy)
            filtered_df_signalData_legacy = new_df_signalData_legacy[signalData_params]
            filtered_df_signalData_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signalData_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signalData_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_legacy = []

            for index, row in filtered_df_signalData_legacy.iterrows():
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                    row['siteTime'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName',
                                    row['tagName'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias',
                                    row['tagAlias'], "-", 'only found in legacy'])
                rows_legacy.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'value',
                                    row['value'], "-", 'only found in legacy'])

            result_df = pd.DataFrame(rows_legacy, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                           'Parameter', 'Legacy', 'Modernized', 'Status'])

            result_df.fillna("-", inplace=True)
            # filtered_df_valueSignalData_legacy = result_df[result_df['Parameter'] == 'value']
            # total_rows = filtered_df_valueSignalData_legacy.shape[0]
            # matches = filtered_df_valueSignalData_legacy[filtered_df_valueSignalData_legacy['Legacy'] == filtered_df_valueSignalData_legacy['Modernized']].shape[0]
            # percent_match = (matches / total_rows) * 100
            df_legacy_count = len(filtered_df_signalData_legacy)
            count_match = (0/df_legacy_count)*100 if df_legacy_count != 0 else "N/A"
            df_summary_signal_data_legacy = pd.DataFrame(
                {
                    'Product Line':
                        df_legacy.loc[(df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0],
                    'Package Name':
                        df_legacy.loc[(df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0],
                    'Package ID': df_legacy.loc[(df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[
                        0],
                    'Legacy Signal Datapoints': len(filtered_df_signalData_legacy),
                    'Modernized Signal Datapoints': 0,
                    'Value Match %': ["N/A"],
                    'Count Match %': [count_match]
                }
            )
            df_summary_signalData_result_legacy = pd.concat(
                [df_summary_signalData_result_legacy, df_summary_signal_data_legacy], ignore_index=True)
            df_result_value_in_legacy = pd.concat([df_result_value_in_legacy, result_df], ignore_index=True)


        elif pd.isnull(df_signalData_legacy['Signal Data'].explode()).all() and not pd.isnull(
                df_signalData_modernized['Signal Data'].explode()).all():

            list_of_signalData_modernized = df_signalData_modernized['Signal Data'].explode()
            new_df_signalData_modernized = pd.json_normalize(list_of_signalData_modernized)
            filtered_df_signalData_modernized = new_df_signalData_modernized[signalData_params]
            filtered_df_signalData_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_signalData_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_signalData_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_modernized = []
            for index, row in filtered_df_signalData_modernized.iterrows():
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                        "-", row['siteTime'], 'only found in modernized'])
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName',
                                        "-", row['tagName'], 'only found in modernized'])
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagAlias',
                                        "-", row['tagAlias'], 'only found in modernized'])
                rows_modernized.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'value',
                                        "-", row['value'], 'only found in modernized'])

            result_df = pd.DataFrame(rows_modernized, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                               'Parameter', 'Legacy', 'Modernized', 'Status'])
            result_df.fillna("-", inplace=True)

            # filtered_df_valueSignalData_modernized = result_df[result_df['Parameter'] == 'value']
            # total_rows = filtered_df_valueSignalData_modernized.shape[0]
            # matches = filtered_df_valueSignalData_modernized[filtered_df_valueSignalData_modernized['Legacy']
            #                                                  == filtered_df_valueSignalData_modernized['Modernized']].shape[0]
            # percent_match = (matches / total_rows) * 100
            df_modernized_count = len(filtered_df_signalData_modernized)
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
                    'Modernized Signal Datapoints': len(filtered_df_signalData_modernized),
                    'Value Match %': ["N/A"],
                    'Count Match %': [count_match]
                }
            )
            df_summary_signalData_result_modernized = pd.concat(
                [df_summary_signalData_result_modernized, df_summary_signal_data_modernized], ignore_index=True)

            df_result_value_in_modernized = pd.concat([df_result_value_in_modernized, result_df], ignore_index=True)

    signalData_result_df = pd.concat(
        [df_result_value_in_both, df_result_value_in_legacy, df_result_value_in_modernized])
    signalData_result_df.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    signalData_result_df.reset_index(drop=True, inplace=True)

    summary_table_signalData = pd.concat([df_summary_signalData_result_legacy, df_summary_signalData_result_modernized,
                                          df_summary_signalData_result_both], ignore_index=True)
    summary_table_signalData.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    # signalData_csv_file = r'output/SignalData_output.csv'
    # signalData_result_df.to_csv(signalData_csv_file, index=True)
    return signalData_result_df, summary_table_signalData


# function to compare Event Data from Legacy vs Modernized DMA json file
def compare_event_data(df_legacy, df_modernized, df_packageID_legacy, df_packageID_modernized):
    """

    :param df_packageID_legacy: str
    :param df_packageID_modernized: str
    :return: csv_filename: str
    """
    package_id_list = np.union1d(df_packageID_legacy, df_packageID_modernized)
    eventData_ignore_params = {'Data Received', "Events Received"}
    eventDatadf_ignore_columns = ['tagId', 'id', "trainId", "downtime", "assemblyName", "eventCategoryId",
                                  "eventDescription",
                                  "currentStateId", "isExternal", "activeTimeStamp",
                                  "eventStateIndicatorId", "tagIdentifier", "customerTagAlias", "tagAlias"]
    df_result_value_in_both = pd.DataFrame()
    df_result_value_in_legacy = pd.DataFrame()
    df_result_value_in_modernized = pd.DataFrame()

    df_summary_eventData_result_both = pd.DataFrame()
    df_summary_eventData_result_legacy = pd.DataFrame()
    df_summary_eventData_result_modernized = pd.DataFrame()
    for eachPackageID in package_id_list:
        df_eventData_legacy = df_legacy.loc[(df_legacy["Package ID"] == eachPackageID,
                                             ['Product Line', 'Package Name', 'Package ID', 'Event Data'])]
        df_eventData_modernized = df_modernized.loc[(df_modernized["Package ID"] == eachPackageID,
                                                     ['Product Line', 'Package Name', 'Package ID', 'Event Data'])]

        # if not (pd.isnull(df_eventData_modernized['Event Data'].explode()).all() and pd.isnull(df_eventData_legacy['Event Data'].explode()).all()):
        if not (pd.isnull(df_eventData_modernized['Event Data'].explode()).all()) and not (
        pd.isnull(df_eventData_legacy['Event Data'].explode()).all()):
            list_of_eventData_legacy = df_eventData_legacy['Event Data'].explode()
            new_df_eventData_legacy = pd.json_normalize(list_of_eventData_legacy)
            filtered_df_eventData_legacy = new_df_eventData_legacy[
                ~new_df_eventData_legacy['tagName'].isin(eventData_ignore_params)]
            filtered_df_eventData_legacy = filtered_df_eventData_legacy.drop(columns=eventDatadf_ignore_columns)
            filtered_df_eventData_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_eventData_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_eventData_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            list_of_eventData_modernized = df_eventData_modernized['Event Data'].explode()
            new_df_eventData_modernized = pd.json_normalize(list_of_eventData_modernized)
            filtered_df_eventData_modernized = new_df_eventData_modernized[
                ~new_df_eventData_modernized['tagName'].isin(eventData_ignore_params)]
            filtered_df_eventData_modernized = filtered_df_eventData_modernized.drop(columns=eventDatadf_ignore_columns)
            filtered_df_eventData_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_eventData_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_eventData_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_eventData_legacy.reset_index()
            df1['value'] = pd.to_numeric(df1['value'], errors='coerce')
            df2 = filtered_df_eventData_modernized.reset_index()
            df2['value'] = pd.to_numeric(df2['value'], errors='coerce')
            merged_df = pd.merge(df1, df2, how='outer', indicator=True)
            # diff_values = merged_df[merged_df['_merge']!='both']
            merged_df['Status'] = merged_df['_merge'].map(
                {'left_only': 'legacy', 'right_only': 'modernized', 'both': 'both'})

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
                elif row['Status'] == 'modernized':
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime', "-",
                         row['siteTime'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp', "-",
                         row['timeStamp'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory', "-",
                         row['eventCategory'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', "-", row['value'],
                         'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', "-",
                         row['tagName'], 'only found in modernized'])
                else:
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime',
                                      row['siteTime'], row['siteTime'], 'found in both'])
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp',
                                      row['timeStamp'], row['timeStamp'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory',
                         row['eventCategory'], row['eventCategory'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', row['value'],
                         row['value'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', row['tagName'],
                         row['tagName'], 'found in both'])

            comparison_df_legacy = pd.DataFrame(rows_legacy,
                                                columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                         'Parameter', 'Legacy', 'Modernized', 'Status'])

            comparison_df_modernized = pd.DataFrame(rows_modernized,
                                                    columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                             'Parameter', 'Legacy', 'Modernized', 'Status'])

            comparison_df_both = pd.DataFrame(rows_both,
                                              columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                       'Parameter', 'Legacy', 'Modernized', 'Status'])

            result_df = pd.concat([comparison_df_legacy, comparison_df_modernized, comparison_df_both],
                                  ignore_index=True)

            if len(df1) != 0 or len(df2) != 0:
                result_df.fillna("-", inplace=True)
                filtered_df_valueEventData_both = result_df[result_df['Parameter'] == 'value']
                total_rows = filtered_df_valueEventData_both.shape[0]
                matches = filtered_df_valueEventData_both[
                    filtered_df_valueEventData_both['Legacy'] == filtered_df_valueEventData_both['Modernized']].shape[0]
                percent_match = (matches / total_rows) * 100

                if len(df1) == 0:
                    count_match = (0/ len(df2)) * 100 if len(df2) != 0 else "N/A"
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
                            'Modernized Event Datapoints': len(df2),
                            'Value Match %': ["N/A"],
                            'Count Match %':[count_match]

                        })
                elif len(df2) == 0:
                    count_match = (0/ len(df1)) * 100 if len(df1) != 0 else "N/A"
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
                            'Modernized Event Datapoints': len(df2),
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
                            'Modernized Event Datapoints': len(df2),
                            'Value Match %': [percent_match],
                            'Count Match %': [count_match]
                        })

                df_summary_eventData_result_both = pd.concat(
                    [df_summary_eventData_result_both, df_summary_event_data_both], ignore_index=True)
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
                        'Modernized Event Datapoints': 0,
                        'Value Match %': ["N/A"],
                        'Count Match %': ["N/A"]
                    })

                df_summary_eventData_result_both = pd.concat(
                    [df_summary_eventData_result_both, df_summary_event_data_both], ignore_index=True)

            df_result_value_in_both = pd.concat([df_result_value_in_both, result_df], ignore_index=True)

        elif pd.isnull(df_eventData_modernized['Event Data'].explode()).all() and not pd.isnull(
                df_eventData_legacy['Event Data'].explode()).all():
            list_of_eventData_legacy = df_eventData_legacy['Event Data'].explode()
            new_df_eventData_legacy = pd.json_normalize(list_of_eventData_legacy)
            filtered_df_eventData_legacy = new_df_eventData_legacy[
                ~new_df_eventData_legacy['tagName'].isin(eventData_ignore_params)]
            filtered_df_eventData_legacy = filtered_df_eventData_legacy.drop(columns=eventDatadf_ignore_columns)
            filtered_df_eventData_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_eventData_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_eventData_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_legacy = []
            df1 = filtered_df_eventData_legacy.reset_index()
            for index, row in filtered_df_eventData_legacy.iterrows():
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
                                                           'Parameter', 'Legacy', 'Modernized', 'Status'])

            if len(df1) != 0:
                count_match = (0 / len(df1)) * 100 if len(df1) != 0 else "N/A"
                result_df.fillna("-", inplace=True)
                # filtered_df_valueEventData_legacy = result_df[result_df['Parameter'] == 'value']
                # total_rows = filtered_df_valueEventData_legacy.shape[0]
                # matches = filtered_df_valueEventData_legacy[
                #     filtered_df_valueEventData_legacy['Legacy'] == filtered_df_valueEventData_legacy['Modernized']].shape[0]
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
                        'Modernized Event Datapoints': 0,
                        'Value Match %': ["N/A"],
                        'Count Match %': [count_match]
                    })
                df_summary_eventData_result_legacy = pd.concat(
                    [df_summary_eventData_result_legacy, df_summary_event_data_legacy], ignore_index=True)

            df_result_value_in_legacy = pd.concat([df_result_value_in_legacy, result_df], ignore_index=True)

        elif pd.isnull(df_eventData_legacy['Event Data'].explode()).all() and not pd.isnull(
                df_eventData_modernized['Event Data'].explode()).all():
            list_of_eventData_modernized = df_eventData_modernized['Event Data'].explode()
            new_df_eventData_modernized = pd.json_normalize(list_of_eventData_modernized)
            filtered_df_eventData_modernized = new_df_eventData_modernized[
                ~new_df_eventData_modernized['tagName'].isin(eventData_ignore_params)]
            filtered_df_eventData_modernized = filtered_df_eventData_modernized.drop(columns=eventDatadf_ignore_columns)
            filtered_df_eventData_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_eventData_modernized.insert(1, 'Package Name', df_legacy.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_eventData_modernized.insert(2, 'Package ID', df_legacy.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_eventData_modernized.reset_index()
            rows_modernized = []
            for index, row in filtered_df_eventData_modernized.iterrows():
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteTime', "-",
                     row['siteTime'], 'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'timeStamp', "-",
                     row['timeStamp'], 'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'eventCategory', "-",
                     row['eventCategory'], 'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'value', "-", row['value'],
                     'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'tagName', "-", row['tagName'],
                     'only found in modernized'])

            result_df = pd.DataFrame(rows_modernized, columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                               'Parameter', 'Legacy', 'Modernized', 'Status'])
            if len(df1) != 0:
                result_df.fillna("-", inplace=True)
                # filtered_df_valueEventData_legacy = result_df[result_df['Parameter'] == 'value']
                # total_rows = filtered_df_valueEventData_legacy.shape[0]
                # matches = filtered_df_valueEventData_legacy[
                #     filtered_df_valueEventData_legacy['Legacy'] == filtered_df_valueEventData_legacy['Modernized']].shape[0]
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
                        'Modernized Event Datapoints': len(df1),
                        'Value Match %': ["N/A"],
                        'Count Match %':[count_match]
                    })
                df_summary_eventData_result_modernized = pd.concat(
                    [df_summary_eventData_result_modernized, df_summary_event_data_modernized], ignore_index=True)
            df_result_value_in_modernized = pd.concat([df_result_value_in_modernized, result_df], ignore_index=True)

        else:
            pass

    eventData_result_df = pd.concat([df_result_value_in_both, df_result_value_in_legacy, df_result_value_in_modernized])
    # eventData_result_df.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    eventData_result_df.reset_index(drop=True, inplace=True)
    summary_table_eventData = pd.concat([df_summary_eventData_result_legacy, df_summary_eventData_result_modernized,
                                         df_summary_eventData_result_both], ignore_index=True)
    # summary_table_eventData.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True], inplace=True)
    # eventData_csv_file = r'output/EventData_output.csv'
    # eventData_result_df.to_csv(eventData_csv_file, index=True)
    return eventData_result_df, summary_table_eventData


# function to compare Agent Messages from Legacy vs Modernized DMA json file
def compare_agent_messages(df_legacy, df_modernized, df_packageID_legacy, df_packageID_modernized):
    """

    :param df_packageID_legacy: str
    :param df_packageID_modernized: str
    :return: csv_filename: str
    """
    package_id_list = np.union1d(df_packageID_legacy, df_packageID_modernized)
    agentMessage_ignore_params = ["creationTime", "hasSpecificText", "agentId", "assemblyId", "categoryId",
                                  "fileIdentifier", "agentSpecifics", "messageTextId",
                                  "agentMessageId", "limitValue", "agentSpecCat", "tagId", "messageMapId"]
    ignore_messageTexts = ['Task chain for DMA Launcher - AGT created by Andreas Hansson',
                           'Task closed manually by Andreas Hansson',
                           'Task opened manually by Andreas Hansson']

    df_result_value_in_both = pd.DataFrame()
    df_result_value_in_legacy = pd.DataFrame()
    df_result_value_in_modernized = pd.DataFrame()
    df_summary_AgentMsgData_result_both = pd.DataFrame()
    df_summary_AgentMsgData_result_legacy = pd.DataFrame()
    df_summary_AgentMsgData_result_modernized = pd.DataFrame()
    for eachPackageID in package_id_list:
        df_agentMessageData_legacy = df_legacy.loc[(df_legacy["Package ID"] == eachPackageID,
                                                    ['Product Line', 'Package Name', 'Package ID', 'Agent Messages'])]
        df_agentMessageData_modernized = df_modernized.loc[(df_modernized["Package ID"] == eachPackageID,
                                                            ['Product Line', 'Package Name', 'Package ID',
                                                             'Agent Messages'])]

        if not pd.isnull(df_agentMessageData_modernized['Agent Messages'].explode()).all() and not pd.isnull(
                df_agentMessageData_legacy['Agent Messages'].explode()).all():

            list_of_agentMessage_legacy = df_agentMessageData_legacy['Agent Messages'].explode()
            new_df_agentMessage_legacy = pd.json_normalize(list_of_agentMessage_legacy)
            for messageText in ignore_messageTexts:
                new_df_agentMessage_legacy = new_df_agentMessage_legacy[
                    ~new_df_agentMessage_legacy['messageText'].str.contains(messageText, regex=True)]
            filtered_df_agentMessage_legacy = new_df_agentMessage_legacy.drop(columns=agentMessage_ignore_params)
            filtered_df_agentMessage_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agentMessage_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agentMessage_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])
            filtered_df_agentMessage_legacy.to_csv(f"filtered_agentmessages_{eachPackageID}.csv")
            list_of_agentMessage_modernized = df_agentMessageData_modernized['Agent Messages'].explode()
            new_df_agentMessage_modernized = pd.json_normalize(list_of_agentMessage_modernized)
            filtered_df_agentMessage_modernized = new_df_agentMessage_modernized.drop(
                columns=agentMessage_ignore_params)
            filtered_df_agentMessage_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agentMessage_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agentMessage_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            df1 = filtered_df_agentMessage_legacy.reset_index()

            df2 = filtered_df_agentMessage_modernized.reset_index()

            merged_df = pd.merge(df1, df2, how='outer', indicator=True)
            # diff_values = merged_df[merged_df['_merge']!='both']
            merged_df['Status'] = merged_df['_merge'].map(
                {'left_only': 'legacy', 'right_only': 'modernized', 'both': 'both'})

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
                elif row['Status'] == 'modernized':
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime', "-",
                         row['siteEventTime'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText', "-",
                         row['messageText'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass', "-",
                         row['messageClass'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity', "-",
                         row['messageSeverity'], 'only found in modernized'])
                    rows_modernized.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope', "-",
                         row['messageScope'], 'only found in modernized'])
                else:
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime',
                         row['siteEventTime'], row['siteEventTime'], 'found in both'])
                    rows_both.append([row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText',
                                      row['messageText'], row['messageText'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass',
                         row['messageClass'], row['messageClass'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity',
                         row['messageSeverity'], row['messageSeverity'], 'found in both'])
                    rows_both.append(
                        [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope',
                         row['messageScope'], row['messageScope'], 'found in both'])

            comparison_df_legacy = pd.DataFrame(rows_legacy,
                                                columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                         'Parameter', 'Legacy', 'Modernized', 'Status'])
            comparison_df_modernized = pd.DataFrame(rows_modernized,
                                                    columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                             'Parameter', 'Legacy', 'Modernized', 'Status'])
            comparison_df_both = pd.DataFrame(rows_both,
                                              columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                       'Parameter', 'Legacy', 'Modernized', 'Status'])

            agentmessage_result_df_both = pd.concat(
                [comparison_df_legacy, comparison_df_modernized, comparison_df_both],
                ignore_index=True)

            if len(df1) != 0 or len(df2) != 0:

                agentmessage_result_df_both.fillna("-", inplace=True)
                filtered_df_msgText_AgentMsgData_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageText']
                filtered_df_msgClass_AgentMsgData_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageClass']
                filtered_df_msgSeverity_AgentMsgData_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageSeverity']
                filtered_df_msgScope_AgentMsgData_both = agentmessage_result_df_both[
                    agentmessage_result_df_both['Parameter'] == 'messageScope']

                total_rows_msgText = filtered_df_msgText_AgentMsgData_both.shape[0]
                total_rows_msgClass = filtered_df_msgClass_AgentMsgData_both.shape[0]
                total_rows_msgSeverity = filtered_df_msgSeverity_AgentMsgData_both.shape[0]
                total_rows_msgScope = filtered_df_msgScope_AgentMsgData_both.shape[0]

                matches_msgText = filtered_df_msgText_AgentMsgData_both[
                    filtered_df_msgText_AgentMsgData_both['Legacy'] == filtered_df_msgText_AgentMsgData_both[
                        'Modernized']].shape[0]
                matches_msgClass = filtered_df_msgClass_AgentMsgData_both[
                    filtered_df_msgClass_AgentMsgData_both['Legacy'] == filtered_df_msgClass_AgentMsgData_both[
                        'Modernized']].shape[0]
                matches_msgSeverity = filtered_df_msgSeverity_AgentMsgData_both[
                    filtered_df_msgSeverity_AgentMsgData_both['Legacy'] == filtered_df_msgSeverity_AgentMsgData_both[
                        'Modernized']].shape[0]
                matches_msgScope = filtered_df_msgScope_AgentMsgData_both[
                    filtered_df_msgScope_AgentMsgData_both['Legacy'] == filtered_df_msgScope_AgentMsgData_both[
                        'Modernized']].shape[0]

                percent_match_msgText = (matches_msgText / total_rows_msgText) * 100

                percent_match_msgClass = (matches_msgClass / total_rows_msgClass) * 100
                percent_match_msgSeverity = (matches_msgSeverity / total_rows_msgSeverity) * 100
                percent_match_msgScope = (matches_msgScope / total_rows_msgScope) * 100
                if len(df1) == 0:
                    df_summary_agentMsg_data_both = pd.DataFrame(
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
                            "msgText Match%": [percent_match_msgText],
                            "msgClass Match%": [percent_match_msgClass],
                            "msgSeverity Match%": [percent_match_msgSeverity],
                            "msgScope Match%": [percent_match_msgScope]
                        })
                elif len(df2) == 0:

                    df_summary_agentMsg_data_both = pd.DataFrame(
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
                            "msgText Match%": [percent_match_msgText],
                            "msgClass Match%": [percent_match_msgClass],
                            "msgSeverity Match%": [percent_match_msgSeverity],
                            "msgScope Match%": [percent_match_msgScope]
                        })
                else:

                    df_summary_agentMsg_data_both = pd.DataFrame(
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
                            "msgText Match%": [percent_match_msgText],
                            "msgClass Match%": [percent_match_msgClass],
                            "msgSeverity Match%": [percent_match_msgSeverity],
                            "msgScope Match%": [percent_match_msgScope]
                        })

                df_summary_AgentMsgData_result_both = pd.concat(
                    [df_summary_AgentMsgData_result_both, df_summary_agentMsg_data_both], ignore_index=True)
            df_result_value_in_both = pd.concat([df_result_value_in_both, agentmessage_result_df_both],
                                                ignore_index=True)


        elif pd.isnull(df_agentMessageData_modernized['Agent Messages'].explode()).all() and not pd.isnull(
                df_agentMessageData_legacy['Agent Messages'].explode()).all():
            list_of_agentMessage_legacy = df_agentMessageData_legacy['Agent Messages'].explode()
            new_df_agentMessage_legacy = pd.json_normalize(list_of_agentMessage_legacy)
            for messageText in ignore_messageTexts:
                new_df_agentMessage_legacy = new_df_agentMessage_legacy[
                    ~new_df_agentMessage_legacy['messageText'].str.contains(messageText, regex=True)]
            filtered_df_agentMessage_legacy = new_df_agentMessage_legacy.drop(columns=agentMessage_ignore_params)

            filtered_df_agentMessage_legacy.insert(0, 'Product Line', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agentMessage_legacy.insert(1, 'Package Name', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agentMessage_legacy.insert(2, 'Package ID', df_legacy.loc[
                (df_legacy['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_legacy = []
            for index, row in filtered_df_agentMessage_legacy.iterrows():
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
                                                                  'Parameter', 'Legacy', 'Modernized', 'Status'])

            result_df_legacy.fillna("-", inplace=True)
            # filtered_df_msgText_AgentMsgData_legacy = result_df_legacy[
            #     result_df_legacy['Parameter'] == 'messageText']
            # filtered_df_msgClass_AgentMsgData_legacy = result_df_legacy[
            #     result_df_legacy['Parameter'] == 'messageClass']
            # filtered_df_msgSeverity_AgentMsgData_legacy = result_df_legacy[
            #     result_df_legacy['Parameter'] == 'messageSeverity']
            # filtered_df_msgScope_AgentMsgData_legacy = result_df_legacy[
            #     result_df_legacy['Parameter'] == 'messageScope']

            # total_rows_msgText = filtered_df_msgText_AgentMsgData_legacy.shape[0]
            # total_rows_msgClass = filtered_df_msgClass_AgentMsgData_legacy.shape[0]
            # total_rows_msgSeverity = filtered_df_msgSeverity_AgentMsgData_legacy.shape[0]
            # total_rows_msgScope = filtered_df_msgScope_AgentMsgData_legacy.shape[0]
            #
            # matches_msgText = filtered_df_msgText_AgentMsgData_legacy[filtered_df_msgText_AgentMsgData_legacy['Legacy'] ==
            #                                                         filtered_df_msgText_AgentMsgData_legacy[
            #                                                             'Modernized'].shape[0]]
            # matches_msgClass = filtered_df_msgClass_AgentMsgData_legacy[
            #     filtered_df_msgClass_AgentMsgData_legacy['Legacy'] ==
            #     filtered_df_msgClass_AgentMsgData_legacy['Modernized'].shape[0]]
            # matches_msgSeverity = filtered_df_msgSeverity_AgentMsgData_legacy[
            #     filtered_df_msgSeverity_AgentMsgData_legacy['Legacy'] ==
            #     filtered_df_msgSeverity_AgentMsgData_legacy['Modernized'].shape[0]]
            # matches_msgScope = filtered_df_msgScope_AgentMsgData_legacy[
            #     filtered_df_msgScope_AgentMsgData_legacy['Legacy'] ==
            #     filtered_df_msgScope_AgentMsgData_legacy['Modernized'].shape[0]]

            # percent_match_msgText = (matches_msgText / total_rows_msgText) * 100
            # percent_match_msgClass = (matches_msgClass / total_rows_msgClass) * 100
            # percent_match_msgSeverity = (matches_msgSeverity / total_rows_msgSeverity) * 100
            # percent_match_msgScope = (matches_msgScope / total_rows_msgScope) * 100
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
                    'Legacy Agent Message Datapoints': len(filtered_df_agentMessage_legacy),
                    'Modernized Agent Message Datapoints': 0,
                    "msgText Match%": ["N/A"],
                    "msgClass Match%": ["N/A"],
                    "msgSeverity Match%": ["N/A"],
                    "msgScope Match%": ["N/A"]
                })

            df_summary_AgentMsgData_result_legacy = pd.concat(
                [df_summary_AgentMsgData_result_legacy, df_summary_agentMsg_data_legacy], ignore_index=True)

            df_result_value_in_legacy = pd.concat([df_result_value_in_legacy, result_df_legacy], ignore_index=True)

        elif (not pd.isnull(df_agentMessageData_modernized['Agent Messages'].explode()).all() and
              pd.isnull(df_agentMessageData_legacy['Agent Messages'].explode()).all()):
            list_of_agentMessage_modernized = df_agentMessageData_modernized['Agent Messages'].explode()
            new_df_agentMessage_modernized = pd.json_normalize(list_of_agentMessage_modernized)
            filtered_df_agentMessage_modernized = new_df_agentMessage_modernized.drop(
                columns=agentMessage_ignore_params)
            filtered_df_agentMessage_modernized.insert(0, 'Product Line', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Product Line')].iloc[0])
            filtered_df_agentMessage_modernized.insert(1, 'Package Name', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package Name')].iloc[0])
            filtered_df_agentMessage_modernized.insert(2, 'Package ID', df_modernized.loc[
                (df_modernized['Package ID'] == eachPackageID, 'Package ID')].iloc[0])

            rows_modernized = []

            for index, row in filtered_df_agentMessage_modernized.iterrows():
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'siteEventTime', "-",
                     row['siteEventTime'], 'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageText', "-",
                     row['messageText'], 'only found in modernized'])

                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageClass', "-",
                     row['messageClass'], 'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageSeverity', "-",
                     row['messageSeverity'], 'only found in modernized'])
                rows_modernized.append(
                    [row['Product Line'], row['Package Name'], row['Package ID'], index, 'messageScope', "-",
                     row['messageScope'], 'only found in modernized'])

            result_df_modernized = pd.DataFrame(rows_modernized,
                                                columns=['Product Line', 'Package Name', 'Package ID', 'Index',
                                                         'Parameter', 'Legacy', 'Modernized', 'Status'])

            result_df_modernized.fillna("-", inplace=True)
            # filtered_df_msgText_AgentMsgData_modernized = result_df_modernized[
            #     result_df_modernized['Parameter'] == 'messageText']
            # filtered_df_msgClass_AgentMsgData_modernized = result_df_modernized[
            #     result_df_modernized['Parameter'] == 'messageClass']
            # filtered_df_msgSeverity_AgentMsgData_modernized = result_df_modernized[
            #     result_df_modernized['Parameter'] == 'messageSeverity']
            # filtered_df_msgScope_AgentMsgData_modernized = result_df_modernized[
            #     result_df_modernized['Parameter'] == 'messageScope']
            #
            # total_rows_msgText = filtered_df_msgText_AgentMsgData_modernized.shape[0]
            # total_rows_msgClass = filtered_df_msgClass_AgentMsgData_modernized.shape[0]
            # total_rows_msgSeverity = filtered_df_msgSeverity_AgentMsgData_modernized.shape[0]
            # total_rows_msgScope = filtered_df_msgScope_AgentMsgData_modernized.shape[0]
            #
            # matches_msgText = filtered_df_msgText_AgentMsgData_modernized[filtered_df_msgText_AgentMsgData_modernized['Legacy'] ==
            #                                                         filtered_df_msgText_AgentMsgData_modernized[
            #                                                             'Modernized'].shape[0]]
            # matches_msgClass = filtered_df_msgClass_AgentMsgData_modernized[
            #     filtered_df_msgClass_AgentMsgData_modernized['Legacy'] ==
            #     filtered_df_msgClass_AgentMsgData_modernized['Modernized'].shape[0]]
            # matches_msgSeverity = filtered_df_msgSeverity_AgentMsgData_modernized[
            #     filtered_df_msgSeverity_AgentMsgData_modernized['Legacy'] ==
            #     filtered_df_msgSeverity_AgentMsgData_modernized['Modernized'].shape[0]]
            # matches_msgScope = filtered_df_msgScope_AgentMsgData_modernized[
            #     filtered_df_msgScope_AgentMsgData_modernized['Legacy'] ==
            #     filtered_df_msgScope_AgentMsgData_modernized['Modernized'].shape[0]]

            # percent_match_msgText = (matches_msgText / total_rows_msgText) * 100
            # percent_match_msgClass = (matches_msgClass / total_rows_msgClass) * 100
            # percent_match_msgSeverity = (matches_msgSeverity / total_rows_msgSeverity) * 100
            # percent_match_msgScope = (matches_msgScope / total_rows_msgScope) * 100
            df_summary_agentMsg_data_modernized = pd.DataFrame(
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
                    'Modernized Agent Message Datapoints': len(filtered_df_agentMessage_modernized),
                    "msgText Match%": ["N/A"],
                    "msgClass Match%": ["N/A"],
                    "msgSeverity Match%": ["N/A"],
                    "msgScope Match%": ["N/A"]
                })

            df_summary_AgentMsgData_result_modernized = pd.concat(
                [df_summary_AgentMsgData_result_modernized, df_summary_agentMsg_data_modernized], ignore_index=True)

            df_result_value_in_modernized = pd.concat([df_result_value_in_modernized, result_df_modernized],
                                                      ignore_index=False)

        else:
            pass

    agentMessageData_result_df = pd.concat(
        [df_result_value_in_both, df_result_value_in_legacy, df_result_value_in_modernized])
    # eventlData_result_df = eventlData_result_df.sort_values(by='Product Line')
    agentMessageData_result_df.reset_index(drop=True, inplace=True)
    summary_table_agentMessageData = pd.concat(
        [df_summary_AgentMsgData_result_legacy, df_summary_AgentMsgData_result_modernized,
         df_summary_AgentMsgData_result_both], ignore_index=True)
    summary_table_agentMessageData.sort_values(by=['Product Line', 'Package Name'], ascending=[True, True],
                                               inplace=True)
    # agentMessageData_csv_file = r'output/AgentMessageData_output.csv'
    # agentMessageData_result_df.to_csv(agentMessageData_csv_file, index=True)
    return agentMessageData_result_df, summary_table_agentMessageData


def compare_output(script_dir, s3_bucket_name, s3_prefix, input_folder_path, timestamp):
    # Input Data
    # Download the input json files Legacy and Modernized from S3 to local
    # download_json_from_s3(s3_bucket_name, s3_prefix, input_folder_path)
    input_folder_path = os.path.join(script_dir, 'input_json')
    legacy_data_json = f'{input_folder_path}/test_DMA_Legacy.json'
    modernized_data_json = f'{input_folder_path}/test_DMA_Modernized.json'
    df_legacy = pd.read_json(legacy_data_json)
    df_modernized = pd.read_json(modernized_data_json)
    df_productLine_legacy = df_legacy['Product Line'].unique()
    df_productLine_modernized = df_modernized['Product Line'].unique()
    df_packageID_legacy = df_legacy['Package ID'].unique()
    df_packageID_modernized = df_modernized['Package ID'].unique()

    # Comparison OutputDataFrames
    # print('Executing Legacy vs Modernized output comparison script')
    # df1 = compare_product_lines(df_productLine_legacy, df_productLine_modernized)
    # print("Comparing Product Lines completed.")
    # df2 = compare_packageNames_for_productLine(df_legacy, df_modernized, df_productLine_legacy,
    #                                            df_productLine_modernized)
    # print("Comparing Package Names completed.")
    df3a_data, df3b_summary = compare_signal_data(df_legacy, df_modernized, df_packageID_legacy,
                                                  df_packageID_modernized)
    # print("Comparing Signal Data completed.")
    # df4a_data, df4b_summary = compare_event_data(df_legacy, df_modernized, df_packageID_legacy, df_packageID_modernized)
    # print("Comparing Event Data completed.")
    # df5a_data, df5b_summary = compare_agent_messages(df_legacy, df_modernized, df_packageID_legacy,
    #                                                  df_packageID_modernized)
    # print("Comparing Agent Messages completed.")

    output_folder_path = os.path.join(script_dir, 'output_csv')
    output_file = os.path.join(output_folder_path, 'output_comparison_data.xlsx')

    print('Saving output to a file..')
    with pd.ExcelWriter(output_file) as writer:
        # df1.to_excel(writer, sheet_name='ProductLine_output', index=False)
        # df2.to_excel(writer, sheet_name='PackageNames_output', index=False)
        df3b_summary.to_excel(writer, sheet_name='SignalData_summary', index=False)
        # df4b_summary.to_excel(writer, sheet_name='EventData_summary', index=False)
        # df5b_summary.to_excel(writer, sheet_name='AgentMessageData_summary', index=False)
        df3a_data.to_excel(writer, sheet_name='SignalData_output', index=False)
        # df4a_data.to_excel(writer, sheet_name='EventData_output', index=False)
        # df5a_data.to_excel(writer, sheet_name='AgentMessageData_output', index=False)

    print('Output files are generated..')
    # Upload output file to s3
    print("Uploading output comparison file to AWS S3..")
    filename = os.path.basename(output_file)
    # upload_comparison_output_file_to_s3(s3_bucket_name, filename, output_file, timestamp)
