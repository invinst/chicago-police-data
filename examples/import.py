import civis
import pandas as import pd
from .connections.dropbox_connection import dropbox_handler
import argparse

client = civis.APIClient()
dropbox = dropbox_handler()

def init_args():
    """Init"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--replace_existing', default=os.environ.get('REPLACE_EXISTING_FILES'))

    return parser.parse_args()


def import_files(replace_existing):
    ## paths where things are in dropbox
    path = '/Data/Roman/Output'
    folders = ['awards','complaints','profiles','ranks','rosters','TRR','unit-history']

    if replace_existing:
        replace='drop'
    else:
        replace='fail'
    ## Database info about the Invisible Institute cluster
    db = 'Invisible Institute'
    db_id = 791
    schema = 'output'
    ## loops through folders and files, imports them to redshift and grants the group
    for folder in folders:
        new_path = '/'.join([path,folder])
        print(new_path)
        upload_files = [file_name for file_name in dropbox.list_files(new_path) if '.csv.gz' in file_name]
        for file_name in upload_files:
            df = dropbox.download_file(new_path,file_name)
            ## unit history tables share names with trrs so adding unit-history to the table name
            if folder == 'unit-history':
                file_name = '-'.join(['unit-history',file_name.split('.')[0]])
                table_name = '.'.join([schema,file_name])
            else:
                table_name = '.'.join([schema,file_name.split('.')[0]])
            print('Importing Table: {}'.format(table_name))
            print('-----------------------------------')
            try:
                import_job = civis.io.dataframe_to_civis(df,
                                                         database=db,
                                                         table = table_name,
                                                         existing_table_rows=replace)
                ## waits for import to be done before starting next import
                import_job.result()
            except:
                print('Import Job Failed: {}'.format(table_name))
                print('Check whether the tables already exist')

    ##grants the group on all tables in schema
    sql_job = 'grant all on all tables in schema {} to group invinst'.format(schema)
    client.queries.post(database=db_id,sql=sql_job,preview_rows=0)

if __name__=="__main__":
    ARGUMENTS = init_args()
    import_files(ARGUMENTS.replace_existing)
