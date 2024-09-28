#!/usr/bin/env python
# Author: Ashwin Sharma

import os
import argparse
import shutil
import matplotlib.pyplot as plt
import pandas as pd
import yaml
import shutil
from nbformat import v4 as nbf
import nbformat

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--file_path", "-f", help="Filename (with path) for csv file to be added.", type=str)
    parser.add_argument("--profiles_flag", "-p", help="True if file contains profiles.", dest="profiles_flag", action="store_true")
    parser.add_argument('--without_profiles_flag', "-n", dest='profiles_flag', action='store_false')
    parser.set_defaults(profiles_flag=False)
    parser.add_argument("--root_path", "-r", help="Root path of Chicago police data repo.", default="../../")

    args = parser.parse_args()

    file_path = args.file_path
    profiles_flag = args.profiles_flag
    root_path = args.root_path

    if not ".csv" in file_path:
        raise ValueError("File must be a csv file.")
    # compress if not compressed, mostly just to keep file name consistent
    elif ".gz" not in file_path:
        df = pd.read_csv(file_path)
        df.to_csv(file_path+".gz", index=False, compression="gzip")

        # remove non compressed file
        os.remove(file_path)
        file_path = file_path + ".gz"

    filename = os.path.basename(file_path)

    # # create basic directory structure with symlinks/scripts/makefiles
    foia_path = create_individual_foia_folder(filename, profiles_flag, root_path)
    shutil.copy(file_path, f"{foia_path}/import/input/")

    # # create notebook (used for updating yamls)
    create_preload_notebook(filename, profiles_flag, os.path.abspath(root_path), foia_path)

def create_individual_foia_folder(filename, profiles_flag, root_path="."):
    """ Main function: given new FOIA data and category, creates an individual folder with relevant structure 
        to start the update process. 

        Specifically:
            * Creates new directory within "individual" directory for this FOIA
            * Creates necessary subdirectories for each task for cleaning this data
            * Adds symlinks from shared code to relevant subdirectories. 
            * Generates base 'setup.py' and makefiles (that may need some manual tweaking)
    """
    share_path = f"{root_path}/share"
    symlink_yaml_path = f"{share_path}/hand/task_symlinks.yaml"

    # make directory for this foia and populate it
    foia_name = filename.split(".csv.gz")[0]
    foia_path = f"{root_path}/individual/{foia_name}"

    os.mkdir(foia_path)
    add_tasks(symlink_yaml_path, share_path, foia_path, root_path, foia_name, profiles_flag)

    return foia_path
    
def add_tasks(symlink_yaml_path, share_path, foia_path, root_path, foia_name, profiles_flag):
    with open(symlink_yaml_path, "r") as f:
        symlink_dict = yaml.safe_load(f)

    # for every task, create subdirectories, add base task script and makefile, add symlinks
    for task in symlink_dict:
        if (not profiles_flag) and (task == 'assign-unique-ids'):
            continue
        else:
            create_task_subdirectory(task, foia_path)

            for sub in symlink_dict[task]:
                for file in symlink_dict[task][sub]:

                    add_symlink(f"{foia_path}/{task}/{sub}", f"{share_path}/{sub}", file)
            
            copy_task_files(f"{task}", share_path, foia_path, foia_name, profiles_flag)

    if profiles_flag:
        add_to_merge(share_path, root_path, foia_name)

def create_task_subdirectory(task_name, foia_path):
    """ Make task directory and subdirectories. """
    os.mkdir(f"{foia_path}/{task_name}")

    os.mkdir(f"{foia_path}/{task_name}/input")
    os.mkdir(f"{foia_path}/{task_name}/hand")
    os.mkdir(f"{foia_path}/{task_name}/output")
    os.mkdir(f"{foia_path}/{task_name}/src")

def add_symlink(task_path, share_path, file, sub_directory=None):
    """ Adds a symlink for a file between the shared folder and the task folder within an optional task sub directory.
    """
    # add subdirectory before filename if exists
    if sub_directory:
        file = f"{sub_directory}/{file}"

    if os.path.exists(f"{share_path}/{file}"):
        os.symlink(f"{os.path.abspath(share_path)}/{file}", f"{task_path}/{file}") # use absolute path/relative doesn't preserve
    else:
        print(f"WARNING: Symlink source does not exist: {share_path}/{file}")

def copy_task_files(task_name, share_path, foia_path, foia_name, profiles_flag):
    """ Copies base task script (e.g., import.py, clean.py) and makefile to relevant task folder. """
    try:
        if profiles_flag:
            # copy files
            shutil.copy(f"{share_path}/hand/task_templates/profile_tasks/{task_name}/{task_name}.py", f"{foia_path}/{task_name}/src/{task_name}.py")
            shutil.copy(f"{share_path}/hand/task_templates/profile_tasks/{task_name}/Makefile", f"{foia_path}/{task_name}/src/Makefile")
        else:
            # copy files
            shutil.copy(f"{share_path}/hand/task_templates/{task_name}/{task_name}.py", f"{foia_path}/{task_name}/src/{task_name}.py")
            shutil.copy(f"{share_path}/hand/task_templates/{task_name}/Makefile", f"{foia_path}/{task_name}/src/Makefile")

        # replace filename in task script and Makefile
        replace_filename_in_file(f"{foia_path}/{task_name}/src/{task_name}.py", foia_name)
        replace_filename_in_file(f"{foia_path}/{task_name}/src/Makefile", foia_name)
        
    except Exception as e:
        print(f"Failed to copy task script with exception: {e}")

def add_to_merge(share_path, root_path, foia_name):
    merge_path = f"{root_path}merge"
    new_merge_number = str(get_max_merge_number(root_path) + 1)

    if len(new_merge_number) == 1:
        new_merge_number = "0" + new_merge_number

    merge_folder = f"{new_merge_number}_{foia_name}"
    os.mkdir(f"{merge_path}/{merge_folder}")

    # make sub directories
    os.mkdir(f"{merge_path}/{merge_folder}/src")
    os.mkdir(f"{merge_path}/{merge_folder}/input")
    os.mkdir(f"{merge_path}/{merge_folder}/output")

    # link files
    for file in ["setup.py", "general_utils.py", "match_functions.py", "merge_functions.py", "update_functions.py"]:
        os.symlink(f"{os.path.abspath(share_path)}/src/{file}", f"{merge_path}/{merge_folder}/src/{file}") # use absolute path/relative doesn't preserve

    shutil.copy(f"{share_path}/hand/task_templates/merge/merge.py", f"{merge_path}/{merge_folder}/src/merge.py")
    shutil.copy(f"{share_path}/hand/task_templates/merge/Makefile", f"{merge_path}/{merge_folder}/src/Makefile")

    replace_filename_in_file(f"{merge_path}/{merge_folder}/src/merge.py", foia_name)
    replace_filename_in_file(f"{merge_path}/{merge_folder}/src/Makefile", foia_name)

def get_max_merge_number(root_path):
    merge_path = f"{root_path}/merge"

    return max([int(folder.split("_")[0]) for folder in os.listdir(merge_path) if folder.split("_")[0].isdigit()])

def replace_filename_in_file(file_path, filename):
    """ Replace {{filename}} with actual filename in provided file, following jinja2 convention for string to be replaced. """
    with open(file_path, "r") as f:
        file_text = f.read()

    file_text = file_text.replace("{{filename}}", filename)

    with open(file_path, "w") as f:
        f.write(file_text)        

def create_preload_notebook(filename, foia_category, root_path, foia_path):
    # notebook object contains cells, either markdown or (python) code, here represented as strings
    # first create them through basic string interpolation, add them to object, then write object to file.
    nb = nbf.new_notebook()

    file = filename.split(".csv.gz")[0]

    markdown_header = f'''# Pre Load {filename} to add it to the existing {foia_category} data.

    The cells below attempt to: 

    1) Generate the correct directory structure, symlinks and makefiles required for adding new data. 
    2) Update the yaml files required for adding data. 
    3) Perform some basic analysis and sanity checks on the data. 

    This notebook was autogenerated with the 'generate_load_notebook.py' script. It's likely that some manual changes are required before the above is done.
    '''

    # note: f string on main triple string means the cell renders in the notebook the actual value
    # f strings within the triple strings, as below, still show up as f strings with the variable in resulting notebook
    import_cell = f'''import sys

sys.path.append("{root_path}/share/src")
from add_foia import *
from preload_functions import * 
import pandas as pd
import numpy as np
import yaml
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option("max_row", 200)
pd.set_option("max_column", 50)

filename = '{filename}'
foia_category = '{foia_category}'
root_path = '{root_path}' 

%matplotlib inline'''

    cells = [nbf.new_markdown_cell(markdown_header), nbf.new_code_cell(import_cell)]

    read_cell = '''share_path = f"{root_path}/share"
column_name_yaml_path = f"{share_path}/hand/column_names.yaml"
column_type_yaml_path = f"{share_path}/hand/column_types.yaml"

with open(column_name_yaml_path, "r") as f:
    column_names = yaml.safe_load(f)

with open(column_type_yaml_path, "r") as f:
    column_types = yaml.safe_load(f)

df = pd.read_csv('../input/' + filename)
df.head()
    '''

    cells.append(nbf.new_code_cell(read_cell))

    create_directory_structure = '''create_individual_foia_folder(filename, foia_category, root_path)'''

    # keep this separate so cell separate from the actual write to yaml to allow user to check results before making the change
    update_yaml_logic = '''column_name_dfs = []

for file in column_names:
    column_name_df = pd.DataFrame(pd.Series(column_names[file])).rename(columns={0: "standardized_column_names"})
    column_name_df["file"] = file

    column_name_dfs.append(column_name_df)

column_name_df = pd.concat(column_name_dfs)
    
new_columns = pd.DataFrame(df.columns).rename(columns={0: "given_column_names"})
mapped_columns = new_columns.merge(column_name_df, left_on="given_column_names", right_index=True, how="left")
mapped_columns["category_match"] = mapped_columns["file"].str.contains(foia_category).fillna(False)

# sort so that possible category matches are first, then keep just first when dropping duplicates so that category match is prioritized
mapped_columns = mapped_columns.sort_values(["given_column_names", "category_match", "file"], ascending=[True, False, True])

mapped_columns = mapped_columns.drop_duplicates(subset=["given_column_names"], keep="first")
missing_mask = mapped_columns["standardized_column_names"].isnull()

# fill in missing ones with just lower case given names
mapped_columns.loc[missing_mask, "standardized_column_names"] = mapped_columns.loc[missing_mask, "given_column_names"].str.lower()

# write just mapping to dict
new_column_dict = mapped_columns.set_index("given_column_names")[["standardized_column_names"]] \
    .rename(columns={"standardized_column_names": filename.split(".csv.gz")[0]}) \
    .to_dict()
    
# add to existing dict and output
column_names.update(new_column_dict)'''

    cells.append(nbf.new_code_cell(update_yaml_logic))

    # simply print out the mapped columns so that any changes can be made
    inspect_column = '''# view actual column mappings
mapped_columns'''

    cells.append(nbf.new_code_cell(inspect_column))

    # actually rename columns
    rename_columns = '''# apply column name changes to df
df.rename(columns=column_names[filename.split('.csv.gz')[0]], inplace=True)'''

    cells.append(nbf.new_code_cell(rename_columns))

    # update yaml file
    update_yaml = '''# actually write to yaml 
with open(column_name_yaml_path, "w") as f:
    yaml.dump(column_names, f)

# convert datetime columns
for column in df.columns:
    if column in column_types:
        if column_types[column] == 'date':
            df[column] = pd.to_datetime(df[column])
    '''

    cells.append(nbf.new_code_cell(update_yaml))

    # analysis cells
    null_percentages = '''get_null_percentages(df)'''

    cells.append(nbf.new_code_cell(null_percentages))

    nulls_per_row = '''# nulls per row stats
df.isnull().sum(axis=1).describe()'''

    cells.append(nbf.new_code_cell(nulls_per_row))

    plot_nulls_per_row = '''# plot number of numbers per row
plot_number_of_nulls_per_row(df)'''

    cells.append(nbf.new_code_cell(plot_nulls_per_row))

    rows_with_one_null = '''# look at rows with just a single null (i.e., which column is null)
df[df.isnull().sum(axis=1) == 1].isnull().sum(axis=0)'''

    cells.append(nbf.new_code_cell(rows_with_one_null))

    rows_with_at_least_one_null = '''# look at rows with > 1 null, but not all null
number_of_cols = df.shape[1]
df[(df.isnull().sum(axis=1) > 1) & (df.isnull().sum(axis=1) < number_of_cols)].isnull().sum(axis=0)'''

    cells.append(nbf.new_code_cell(rows_with_at_least_one_null))

    date_stats = '''# some stats on date columns 
df.describe(include=["datetime64"], datetime_is_numeric=True).T'''

    cells.append(nbf.new_code_cell(date_stats))

    plot_date_distributions = '''# plot the month-year counts for every datetime column
for column in df.select_dtypes(include=["datetime64"]).columns:
    column_name = " ".join([word.title() for word in column.split("_")])
    
    title = f"{column_name} - Year Month Counts"
    plot_year_month_counts(df, column, title)'''

    cells.append(nbf.new_code_cell(plot_date_distributions))

    possible_categories = '''# list of possible category columns (unique values between 2 and 20)
get_possible_categories(df)'''

    cells.append(nbf.new_code_cell(possible_categories))

    possible_categories_with_values = '''# possible columns from above but with the actual values 
get_possible_categories_with_values(df)'''

    cells.append(nbf.new_code_cell(possible_categories_with_values))

    nb["cells"] = cells

    with open(f"{foia_path}/import/src/{file}_preload_analysis.ipynb", 'w') as f:
        nbformat.write(nb, f)

# helper functions
def get_null_percentages(df):
    '''Outputs percentage of nulls per column of df along with counts.
    '''
    nulls = pd.DataFrame(df.isnull().sum(axis=0)).rename(columns={0: "nulls"})
    
    nulls["total"] = df.shape[0]
    
    nulls["null percentage"] = nulls["nulls"] / nulls["total"]
    
    return nulls[["nulls", "total", "null percentage"]].style \
        .format({"null percentage": "{:.2%}"}) \
        .bar(color="lightcoral", vmin=0, vmax=1, subset=["null percentage"], align="zero")

def plot_number_of_nulls_per_row(df, title="Count of Number of Nulls Per Row"):
    '''Plots the number of rows that have n number of nulls. 
    E.g., how many rows have no nulls, how many have 1 null, etc. etc. 
    '''
    fig, ax = plt.subplots()

    ax.set_title(title)
    ax.set_xlabel("Number of Nulls")
    ax.set_ylabel("Row Count")

    df.isnull().sum(axis=1).value_counts().sort_index().plot.bar()

    plt.show()

# helper functions
def get_possible_categories(df):
    nulls = df.isnull().sum(axis=0).T
    nulls.name = "null count"

    unique = df.nunique()
    unique.name = "unique"

    stats = pd.DataFrame(nulls).merge(unique, left_index=True, right_index=True, how="outer") 

    stats["count"] = df.shape[0]
    return list(stats[(stats["unique"] <= 20) & (stats["unique"] > 2) & (stats["null count"] != stats["count"])].index) 

def get_possible_categories_with_values(df):
    possible_categories = get_possible_categories(df)

    mapped_categories = []

    for column in possible_categories:
        value_counts = df[column].value_counts()

        # create a mult index from column name and possible categories
        value_counts.index = pd.MultiIndex.from_product([[column], list(value_counts.index)])

        mapped_categories.append(value_counts)

    return pd.DataFrame(pd.concat(mapped_categories)).rename(columns={0: "Count"})

# helper functions
def plot_year_month_counts(df, column, title="Year-Month Counts", uid=None):
    fig, ax = plt.subplots()
    fig.set_size_inches(80, 20)
    
    ax.set_title(title)
    
    ax.set_xlabel("Year - Month")
    ax.set_ylabel("Count")
    
    # drop duplicates based on uid if provided
    if uid:
        df = df[[uid, column]].drop_duplicates()
    
    # summarize to month-year, get counts and plot
    df[column].dt.to_period("M").value_counts().sort_index().plot.bar(ax=ax)
    plt.show()

if __name__ == '__main__':
    main()