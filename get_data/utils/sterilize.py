#!usr/bin/env python3
#
# Author(s): RR

'''script to sterilize TRR data'''

import pandas as pd
import numpy as np
import xlsxwriter
import logging


def clean_cb(x):
    if isinstance(x, str):
        x = x.replace(".0", "").replace("0.", "").replace("-", "")
        if len(set(x)) == 1 or x == "12345678":
            return np.nan
        else:
            return int(x)
    elif np.isnan(x):
        return np.nan
    elif x < 10:
        return np.nan
    else:
        return int(x)


def process_data(filename):
    xl_wb = pd.ExcelFile(filename)
    logging.info("File loaded")

    # Sterilize CB NOs
    sheet_name = "Sheet1"
    try:
        main_df = xl_wb.parse(sheet_name)
    # sheet1 isn't always the name
    except:
        sheet_name = 'TRRData'
        main_df = xl_wb.parse(sheet_name)
    charges_df = xl_wb.parse("Charges")
    logging.info("Sheets loaded")

    main_df['SUBJECT_CB_NO'] = main_df['SUBJECT_CB_NO'].map(clean_cb)
    charges_df['SUBJECT_CB_NO'] = charges_df['SUBJECT_CB_NO'].map(clean_cb)
    logging.info("CBs cleaned")

    cb_df = pd.DataFrame(main_df['SUBJECT_CB_NO']
                         .append(charges_df['SUBJECT_CB_NO']),
                         columns=["SUBJECT_CB_NO"]) \
        .dropna() \
        .drop_duplicates() \
        .reset_index(drop=True)
    cb_df.insert(0, 'SUBJECT_NO', cb_df.index+1)
    logging.info("cb_df formed")

    main_df = main_df.merge(cb_df, on="SUBJECT_CB_NO", how="left")
    charges_df = charges_df.merge(cb_df, on="SUBJECT_CB_NO", how="left")

    assert (main_df[['SUBJECT_CB_NO', 'SUBJECT_NO']]
            .dropna(how='any')
            .drop_duplicates().shape[0]) == main_df['SUBJECT_CB_NO'].nunique()

    assert (charges_df[['SUBJECT_CB_NO', 'SUBJECT_NO']]
            .dropna(how='any')
            .drop_duplicates().shape[0]) == charges_df['SUBJECT_CB_NO'] \
        .nunique()
    logging.info("subject_no's added to dfs")

    # Sterilize RD_NO
    rd_df = pd.DataFrame(main_df['RD_NO'].append(charges_df['RD_NO']),
                         columns=["RD_NO"])\
        .replace('NONE', np.nan)\
        .dropna()\
        .drop_duplicates()\
        .reset_index(drop=True)
    rd_df.insert(0, 'SR_NO', rd_df.index+1)
    logging.info("rd_df formed")

    main_df = main_df.merge(rd_df, on="RD_NO", how="left")
    charges_df = charges_df.merge(rd_df, on="RD_NO", how="left")

    assert (main_df[['RD_NO', 'SR_NO']]
            .dropna(how='any')
            .drop_duplicates().shape[0]) == \
        (main_df['RD_NO']
            .replace('NONE', np.nan)
            .nunique())
    assert (charges_df[['RD_NO', 'SR_NO']]
            .dropna(how='any')
            .drop_duplicates().shape[0]) == \
        (charges_df['RD_NO']
            .replace('NONE', np.nan)
            .nunique())
    assert not {'NONE'} & set(main_df['SR_NO'])
    assert not {'NONE'} & set(charges_df['SR_NO'])

    logging.info("SR_NO's added to dfs")

    # Sterilize EVENT_NO
    event_df = main_df[['EVENT_NO']]\
        .replace(0, np.nan)\
        .dropna(how='any')\
        .drop_duplicates()\
        .reset_index(drop=True)
    event_df.insert(0, 'SE_NO', event_df.index+1)
    logging.info("event_df formed")

    main_df = main_df.merge(event_df, on="EVENT_NO", how="left")

    assert (main_df[['EVENT_NO', 'SE_NO']]
            .dropna(how='any')
            .drop_duplicates().shape[0]) == \
        (main_df['EVENT_NO']
            .replace(0, np.nan)
            .nunique())
    assert not {0} & set(main_df['SE_NO'])
    logging.info("SE_NO's added to dfs")
    return xl_wb, main_df, charges_df


# Write files
def write_files(output_filename,
                main_df,
                charges_df,
                xl_wb):

    writer = pd.ExcelWriter(output_filename, engine='xlsxwriter')
    logging.info("writer opened")

    del main_df["SUBJECT_CB_NO"]
    del main_df["CR_NO_OBTAINED"]
    del main_df["RD_NO"]
    del main_df["EVENT_NO"]

    main_df.to_excel(writer, 'Sheet1', index=False)
    logging.info("main_df written")

    del charges_df["SUBJECT_CB_NO"]
    del charges_df["RD_NO"]

    charges_df.to_excel(writer, 'Charges', index=False)
    logging.info("charges_df written")

    for sn in xl_wb.sheet_names:
        if sn not in ["Sheet1", "TRRData", "Charges"]:
            xl_wb.parse(sn).to_excel(writer, sn, index=False)
            logging.info("{} sheet written".format(sn))

    logging.info("Saving...")
    writer.save()
    logging.info("Done.")
    return xl_wb.sheet_names


def sterilize(filename):
    xl_wb, main_df, charges_df = process_data(filename)
    output_filename = filename.split('.')[0] + '_sterilized.xlsx'
    logging.info(f'MAIN COLUMNS: {main_df.columns}')
    trr_files = write_files(output_filename,
                            main_df,
                            charges_df,
                            xl_wb)
    return output_filename, trr_files
