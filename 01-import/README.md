##Check all files for
##Date Conversion Code:
##at the top
    col_list = df.columns.tolist()
    Report_Produced_Date = [x for x in col_list if isinstance(x, datetime.datetime)]
    col_list = [x for x in col_list if isinstance(x, datetime.datetime)==False]
    FOIA_Request = [x for x in col_list if 'FOIA' in x][0]

##at end
try:
      df["Report_Produced_Date"]=Report_Produced_Date[0].date()
  except:
      df["Report_Produced_Date"]=''

## CRID as Int Conversion

## Make sure files append to final_df

## Check str.replace vs replace

##
