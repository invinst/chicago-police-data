"""CLI tool for adding FOIAs to the CPDP Data Pipeline"""
__version__ = "0.1"


import click
from mako.template import Template
from mako.lookup import TemplateLookup
import os
import traceback
import pandas as pd
from openpyxl import load_workbook
import textwrap
from nbformat import v4 as nbf
import nbformat
import shutil
import yaml

author="Ashwin Sharma (Invisible Institute)"
root_path = "."
individual_path = f"{root_path}/individual"
share_path = f"{root_path}/share"

foia_type_options = ["awards", "complaints", "roster", "salary", "settlements", "TRR", "unit-history"]
sub_type_options = {
    "complaints": ["accused", "complaints", "civilian-witnesses", 'complainants', 'CPD-witnesses', 'investigators', 'victims', 'witnesses'],
    "TRR": ["actions-responses", "charges", "main", "officers", "statuses", "subject-weapons", "subjects", "weapon-discharges"]}
agency_options = {
    "complaints": ["clear", "cms", "crms", ""]}
id_cols = ['first_name', 'last_name', 'first_name_NS', 'last_name_NS',
            'middle_initial','middle_initial2', 'suffix_name',
            'birth_year', 'race', 'gender', 'appointed_date', 'cr_id', 'trr_id']
incident_cols = ['rank', 'current_unit', 'star']

template_lookup = TemplateLookup(f"{share_path}/templates")

with open(f"{root_path}/share/hand/task_symlinks.yaml", "r") as f:
    symlink_dict = yaml.safe_load(f)

column_name_yaml_path = f"{share_path}/hand/column_names.yaml"

with open(column_name_yaml_path, "r") as f:
    column_names = yaml.safe_load(f)

def get_idx_for_param(ctx, param_name): 
    idxs = [idx for idx, param in enumerate(ctx.command.params) if param.name == param_name]
    if idxs:
        return idxs[0]
    else:
        raise ValueError(f"Param name {param_name} not found.")

def require_subtype(ctx, param, value, param_name, option_dict):
    if value in option_dict:
        title_param = " ".join([x.title() for x in value.replace("foia", "FOIA").split("_")])
        prompt = f"Subtype for {value}"
        subtype_idx = get_idx_for_param(ctx, param_name)
        ctx.command.params[subtype_idx].required = True
        ctx.command.params[subtype_idx].prompt = prompt
        ctx.command.params[subtype_idx].type = click.Choice(option_dict[value])

def require_subtypes(ctx, param, value):
    require_subtype(ctx, param, value, "foia_subtype", sub_type_options)
    require_subtype(ctx, param, value, "agency", agency_options)
    return value

def disable_other_options(ctx, param, value):
    # if given full foia name, don't prompt for anything else
    if value:
        for p in ctx.command.params:
            if p.name in ("foia_type", "foia_subtype", 'agency', "start_year", "end_year", "date_received", "foia_number"):
                p.required = False
                p.prompt = None
                p.value = None
        return value

def validate_file_type(ctx, param, value):
    # validate that file type is either excel or csv
    if value.endswith(".csv"):
        return value
    if (".xls" in value) or ('.xlsx' in value):
        excel_file = pd.ExcelFile(value)
        sheet_names = excel_file.sheet_names
        sheet_param_idx = get_idx_for_param(ctx, "sheet")
        if len(sheet_names) == 1:
            ctx.command.params[sheet_param_idx].value = sheet_names[0]
            return value
        else:
            ctx.command.params[sheet_param_idx].required = True
            ctx.command.params[sheet_param_idx].prompt = "Choose sheet for excel file"
            ctx.command.params[sheet_param_idx].type = click.Choice(excel_file.sheet_names)
            return value

@click.group()
def cpdp():
    """General utility script for adding new data to this project. 
    Attempts to give a starting point for adding new FOIA files by building 
    directory structure, giving basic scripts for each task, and adding symlinks"""
    pass

@click.command()
@click.argument('file', type=click.Path(exists=True), metavar="file", callback=validate_file_type)
@click.option('--sheet', default="", required=False)
@click.option('--profile', is_flag=True, prompt="Does this file contain officer profiles?", help="Flag for if this FOIA contain officer profiles")
@click.option('--author', default=author, help="Your name/author of code")
@click.option('--foia_name', help="Optional: full foia name to be used", callback=disable_other_options)
@click.option('--foia_type', prompt="Type of FOIA", help="Type of FOIA, e.g., complaints, salary, TRR", type=click.Choice(foia_type_options), callback=require_subtypes)
@click.option('--foia_subtype', required=False, help="Subtype of foia, e.g., accused for complaints") # see callback for foia_type
@click.option('--agency', required=False, default="") # see callback for foia_type
@click.option('--start_year', prompt="First year the data represents", help="First year the data represents")
@click.option('--end_year', prompt="Last year the data represents", help="Last year the data represents")
@click.option('--date_received', prompt="Year-Month received, i.e., 2016-08")
@click.option('--foia_number', prompt="FOIA number", default="", help="FOIA number, e.g., p506887")
def add(file, sheet, profile, author, foia_name, foia_type, foia_subtype, agency, start_year, end_year, date_received, foia_number):
    """Main function for adding FOIAs. Prompts for filename, if this file contains profiles, general FOIA information, 
    adds FOIA to indiviudal (and merge if containing profiles), adds tasks for each. 

    file: foia file to add. 
    """
    input_file = os.path.basename(file).replace(" ", "_")

    if not foia_name:
        base_foia = foia_type
        if foia_subtype:
            base_foia += f"-{foia_subtype}"
        
        if agency:
            base_foia += f"-{agency}"

        foia_name = f"{base_foia}_{start_year}-{end_year}_{date_received}"

        if foia_number:
            foia_name += f"_{foia_number}"
    else:
        # if just given foia_name directly, get foia type by parsing string
        foia_type = foia_name.split("_")[0].split("-")[0]
    
    columns = get_column_names(file, sheet)

    click.confirm(f"Add individual folder {foia_name}", abort=True)
    try:
        foia_path = create_individual_foia_folder(foia_name)
    except Exception as e:
        click.echo(f"Failed to create individual folder with exception {e}. Exiting")
        return
    click.echo("Created individual folder")

    click.echo("Adding this foia to share/hand/column_names.yaml for renaming columns")
    click.echo("Looking up column names against existing column names to guess possible changes")
    try:
        new_columns = update_column_name_yaml(foia_name, foia_type, columns)
    except Exception as e:
        click.echo(f"Failed to update yaml file with exception {e}", err=True)
        click.confirm("Delete created individual folder?", abort=True)
        delete_individual_foia_folder(foia_name)

    global id_cols
    global incident_cols
    id_cols = [col for col in id_cols if col in new_columns]
    incident_cols = [col for col in incident_cols if col in new_columns]

    # add in NS (no space) and extra columns created by name clean functions
    if "first_name" in id_cols:
        id_cols.append("first_name_NS")
    
    if "last_name" in id_cols:
        id_cols.append("last_name_NS")

    if "middle_initial" in id_cols:
        id_cols.append("middle_initial2")

    template_vars = {
        "author": author,
        "profile": profile,
        "foia_name": foia_name,
        "input_file": input_file, 
        "sheet": sheet, 
        "id_cols": id_cols,
        "incident_cols": incident_cols}

    click.echo("Adding individual tasks")
    try:
        add_individual_tasks(foia_path, profile, template_vars)
    except Exception as e:
        click.echo(f"Failed to add tasks with exception {e}", err=True)
        click.confirm("Delete created individual folder?", abort=True)
        delete_individual_foia_folder(foia_name)
        return

    click.echo("Adding symlinks between tasks")
    try:
        symlink_individual_tasks(foia_path, profile, foia_name)
    except Exception as e:
        click.echo(f"Failed to add symlinks with exception {e}", err=True)
        click.confirm("Delete created individual folder?", abort=True)
        delete_individual_foia_folder(foia_name)
        return

    click.echo(f"Adding file {input_file} to import in individual folder")
    try:
        shutil.copy(file, f"{foia_path}/import/input/{input_file}")
    except Exception as e:
        click.echo(f"Failed to add input file to import/input directory.")
        click.confirm("Delete created individual folder?", abort=True)
        delete_individual_foia_folder(foia_name)
        return

    click.echo(f"Adding makefile for running tasks in folder")
    try:
        render_individual_makefile(foia_path, template_vars)
    except Exception as e:
        click.echo(f"Failed to create makefile with exception {e}")
        click.confirm("Delete created individual folder?", abort=True)
        delete_individual_foia_folder(foia_name)
        return

    if profile:
        if click.confirm("Add merge folder?", abort=True):
            add_to_merge(root_path, foia_name, share_path, author)

@click.command()
@click.option('--author', default=author, help="Your name/author of code")
@click.option('--foia_name', help="Optional: full foia name to be used")
def merge(foia_name, author):
    """Add FOIA already in individual folder to merge"""
    add_to_merge(root_path, foia_name, share_path, author)

@click.command()
@click.option('--author', default=author, help="Your name/author of code")
@click.option('--foia_name', help="FOIA to be updated")
@click.option('--task', type=click.Choice(['import', 'clean', 'assign-unique-ids', 'export', 'merge']))
def notebook(author, foia_name, task):
    """Adds notebook to existing foia task"""
    template_vars = {
        "author": author,
        "foia_name": foia_name}

    if task == "merge":
        merge_path = f"{root_path}/merge"
        merge_folders = {"_".join(folder.split("_")[1:]): int(folder.split("_")[0]) for folder in os.listdir(merge_path) 
                        if folder[0].isdigit()}

        if not foia_name in merge_folders:
            raise FileNotFoundError(f"{foia_name} not found in merge.")

        merge_number = merge_folders[foia_name]
        foia_path = f"{merge_path}/{merge_number}_{foia_name}"
        task_path = foia_path

        template_vars["merge_number"] = merge_number
    else:
        foia_path = f"individual/{foia_name}"
        task_path = f"individual/{foia_name}/{task}"

    if task == "assign-unique-ids":
        try:
            input_file = f"individual/{foia_name}/assign-unique-ids/input/{foia_name}.csv.gz"
            cols = get_column_names(input_file)
            template_vars["id_cols"] = cols
            template_vars["incident_cols"] = []
        except Exception as e:
            click.echo("Unable to read file to get column names. Using defaults")
            template_vars["id_cols"] = id_cols
            template_vars["incident_cols"] = []

    if not "note" in os.listdir(task_path):
        os.mkdir(f"{task_path}/note") 

    if f"{task}.ipynb" in os.listdir(f"{task_path}/note"):
        raise FileExistsError(f"{task}.ipynb already exists.")
    render_notebook_for_task(task, foia_path, template_vars)

cpdp.add_command(add)
cpdp.add_command(merge)
cpdp.add_command(notebook)

def delete_individual_foia_folder(foia_name):
    shutil.rmtree(f"{individual_path}/{foia_name}")

def create_individual_foia_folder(foia_name):
    """ create base individual folder
    """
    if "individual" not in os.listdir(root_path):
        raise FileNotFoundError("Individual folder not found in given root directory.")

    indiviudal_path = f"{root_path}/individual"

    # make directory for this foia and populate it
    if foia_name in os.listdir(indiviudal_path):
        click.echo("Folder already exists")
        click.confirm("Replace it?")
        delete_individual_foia_folder(foia_name)

    foia_path = f"{root_path}/individual/{foia_name}"
    os.mkdir(foia_path)
    return foia_path

def add_individual_tasks(foia_path, profiles_flag, template_vars):
    # for every task, create subdirectories, add base task script and makefile, add symlinks to shared
    for task in symlink_dict:
        if (not profiles_flag) and (task == 'assign-unique-ids'):
            continue
        else:
            create_task_subdirectory(task, foia_path)

            for sub in symlink_dict[task]:
                for file in symlink_dict[task][sub]:
                    symlink_shared_to_task(f"{share_path}/{sub}", f"{foia_path}/{task}/{sub}", file)

            render_task_script(task, foia_path, template_vars)
            render_notebook_for_task(task, foia_path, template_vars)
            click.echo(f"Added task {task}")

def symlink_individual_tasks(foia_path, profiles_flag, foia_name):
    tasks = [task for task in symlink_dict if (task != 'assign-unique-ids') or profiles_flag]

    prev_task = ""
    for task in tasks:
        if not prev_task:
            prev_task = task
        else:
            symlink_tasks(prev_task, task, foia_path, foia_name, profiles_flag)
            prev_task = task

def create_task_subdirectory(task_name, foia_path):
    """ Make task directory and subdirectories. """
    os.mkdir(f"{foia_path}/{task_name}")

    os.mkdir(f"{foia_path}/{task_name}/input")
    os.mkdir(f"{foia_path}/{task_name}/hand")
    os.mkdir(f"{foia_path}/{task_name}/output")
    os.mkdir(f"{foia_path}/{task_name}/src")
    os.mkdir(f"{foia_path}/{task_name}/note")

def symlink_shared_to_task(share_path,task_path, file, sub_directory=None):
    """ Adds a symlink for a file between the shared folder and the task folder within an optional task sub directory.
    """
    # add subdirectory before filename if exists
    if sub_directory:
        file = f"{sub_directory}/{file}"

    if os.path.exists(f"{share_path}/{file}"):
        os.symlink(f"{os.path.abspath(share_path)}/{file}", f"{task_path}/{file}") # use absolute path/relative doesn't preserve
    else:
        print(f"WARNING: Symlink source does not exist: {share_path}/{file}")

def symlink_tasks(prev_task, task, foia_path, foia_name, profiles_flag):
    # create symlink from prev_task/output/foia_name.csv.gz to task/input/foia_name.csv.gz
    # note that symlink can be empty/not reference anything if soft link
    source_path = f"{os.path.abspath(foia_path)}/{prev_task}/output/"
    dest_path = f"{foia_path}/{task}/input/"
    os.symlink(f"{source_path}/{foia_name}.csv.gz", f"{dest_path}/{foia_name}.csv.gz")

    if (task == "export") and profiles_flag:
        os.symlink(f"{source_path}/{foia_name}_profiles.csv.gz", f"{dest_path}/{foia_name}_profiles.csv.gz")

def render_task_script(task_name, foia_path, template_vars):
    """ Creates base task script (e.g., import.py, clean.py) to relevant task folder. """
    src_path = f"{foia_path}/{task_name}/src"
    render_template_to_path(template_name=f"{task_name}.py.mako", path=f"{src_path}/{task_name}.py", template_vars=template_vars)

def render_individual_makefile(foia_path, template_vars):
    """ Creates makefile for individual folder"""
    render_template_to_path(template_name="Makefile.mako", path=f"{foia_path}/Makefile", template_vars=template_vars)    

def render_template_to_path(template_name, path, template_vars):
    try:
        global template_lookup
        template = template_lookup.get_template(template_name)

        with open(path, "w") as f:
            f.write(template.render(**template_vars))
    except Exception as e:
        raise ValueError(f"Failed to write {template} to path {path} with exception {e}")

def update_column_name_yaml(foia_name, foia_type, columns):
    # parse through existing column mappings from past FOIAs from yaml
    # try and match new columns to existing columns to mimic old mapping
    # if not found, at least sanitize column names

    column_name_dfs = []

    for foia in column_names:
        df = pd.DataFrame(pd.Series(column_names[foia])).rename(columns={0: "standardized_column_names"})
        df["foia"] = foia

        column_name_dfs.append(df)

    column_name_df = pd.concat(column_name_dfs)
        
    # if foia already exists, skip
    if foia_name in column_name_df.foia.values:
        click.echo("FOIA already in column name df, skipping step")
        return column_name_df.loc[column_name_df.foia == foia_name, "standardized_column_names"].values

    new_columns = pd.DataFrame(columns).rename(columns={0: "given_column_names"})
    mapped_columns = new_columns.merge(column_name_df, left_on="given_column_names", right_index=True, how="left")
    mapped_columns["category_match"] = mapped_columns["foia"].str.contains(foia_type).fillna(False)

    # sort so that possible category matches are first, then keep just first when dropping duplicates so that category match is prioritized
    mapped_columns = mapped_columns.sort_values(["given_column_names", "category_match", "foia"], ascending=[True, False, True])

    mapped_columns = mapped_columns.drop_duplicates(subset=["given_column_names"], keep="first")
    missing_mask = mapped_columns["standardized_column_names"].isnull()

    # fill in missing ones with just lower case given names
    mapped_columns.loc[missing_mask, "standardized_column_names"] = mapped_columns.loc[missing_mask, "given_column_names"].str.lower().str.replace(" ", "_")

    # write just mapping to dict
    new_column_dict = mapped_columns.set_index("given_column_names")[["standardized_column_names"]] \
        .rename(columns={"standardized_column_names": foia_name}) \
        .to_dict()

    click.echo("Proposed mapping:")
    click.echo(mapped_columns.to_string())
    click.confirm("Proceed to write to column_names.yaml?", abort=True)

    with open(column_name_yaml_path, "a") as f:
        f.write("\n\n")
        yaml.dump(new_column_dict, f)

    return new_column_dict[foia_name].values()

def render_notebook_for_task(task, foia_path, template_vars):
    # pick out relevant blocks of code from task, render it to cells of a notebook for processing
    # manually pull out setup.py so that args, log and cons work as normal
    nb = nbf.new_notebook()
    cells = []

    cells.append(nbf.new_markdown_cell(f"# {task} for {template_vars['foia_name']}"))
    cells.append(nbf.new_markdown_cell(f"## Imports/Setup"))
    # get actual template code, use named blocks to segment out cells of notebook
    template = template_lookup.get_template(f"{task}.py.mako")

    block_names = ["import_block", "args", "input", "process", "output"]
    blocks = {block_name: template.get_def(block_name).render(**template_vars) for block_name in block_names}

    indented_blocks = ["args", "input", "process", "output"]
    blocks = {block_name: textwrap.dedent(block) if block_name in indented_blocks else block 
            for block_name, block in blocks.items()}

    imports = ["os", "sys", "yaml", "logging", "__main__", "importlib", "matplotlib.pyplot as plt", "seaborn as sns"]
    subdirectory_paths = ["src", "hand", "input", "output"]

    # imports/add paths
    setup_cell = [f"import {module}" for module in imports]

    setup_cell += ["from collections import namedtuple", ""]
    if task == "merge":
        setup_cell += [f"os.chdir('{os.path.abspath(foia_path)}')"]
    else:
        setup_cell += [f"os.chdir('{os.path.abspath(foia_path)}/{task}')"]
    setup_cell += [f"sys.path.append('{path}')" for path in subdirectory_paths]

    # get standard import block for this task from template
    setup_cell += [blocks['import_block']] 
    setup_cell += [f"__main__.__file__ = 'src/{task}.py'"]
    setup_cell += [f"setup = importlib.import_module('{task}')", "cons, log = setup.get_setup()"]
    setup_cell += ["sns.set_theme()", "sns.set_style('darkgrid')", "pd.set_option('max_row', 100)", "%matplotlib inline"]
    cells.append(nbf.new_code_cell("\n".join(setup_cell)))

    cells.append(nbf.new_markdown_cell("## Read input "))
    input_cell = [blocks['input']]
    if task == "merge":
        input_cell += ["sup_df.head()"]
    else:
        input_cell += ["df.head()"]
    cells.append(nbf.new_code_cell("\n".join(input_cell)))

    cells.append(nbf.new_markdown_cell(f"## {task}"))
    cells.append(nbf.new_code_cell(blocks['process']))

    if task == "merge":
        cells.append(nbf.new_markdown_cell("## Merge Report"))
        merge_cell = ["rd.generate_merge_report()"]
        merge_cell += ["rd.merge_report()"]

    cells.append(nbf.new_markdown_cell("## Override Args"))
    cells.append(nbf.new_markdown_cell("Convenience to override default args from task file"))
    arg_cell = [blocks["args"], "constants = {'csv_opts': {'index': False, 'compression': 'gzip'}}",
            "constants.update(args)", "cons = namedtuple('Arguments', constants.keys())(**constants)"]
    cells.append(nbf.new_code_cell("\n".join(arg_cell)))

    cells.append(nbf.new_markdown_cell(f"## Output"))
    output_cell = blocks['output']
    cells.append(nbf.new_code_cell(output_cell))

    nb["cells"] = cells

    if task == "merge":
        if not "note" in os.listdir(foia_path):
            os.mkdir(f"{foia_path}/note")

        filepath = f"{foia_path}/note/merge.ipynb"
    else:
        filepath = f"{foia_path}/{task}/note/{task}.ipynb"

    with open(filepath, 'w') as f:
        nbformat.write(nb, f)

def get_max_merge_number(root_path):
    merge_path = f"{root_path}/merge"

    return max([int(folder.split("_")[0]) for folder in os.listdir(merge_path) if folder.split("_")[0].isdigit()])

def get_max_merge_path(root_path, max_merge_number=None):
    if not max_merge_number:
        max_merge_number = get_max_merge_number(root_path)

    merge_path = f"{root_path}/merge"

    return [folder for folder in os.listdir(merge_path) if folder.split("_")[0].isdigit() and int(folder.split("_")[0]) == max_merge_number][0]

def add_to_merge(root_path, foia_name, share_path, author):
    individual_path = f"{root_path}/individual"
    merge_path = f"{root_path}/merge"
    max_merge_number = get_max_merge_number(root_path)
    new_merge_number = str(max_merge_number + 1)
    max_merge_folder = get_max_merge_path(root_path, max_merge_number)

    if not os.path.exists(f"{individual_path}/{foia_name}"):
        raise FileNotFoundError(f"FOIA {foia_name} not found in individual folder. Can't add merge.")

    if len(new_merge_number) == 1:
        new_merge_number = "0" + new_merge_number

    merge_folder = f"{new_merge_number}_{foia_name}"
    os.mkdir(f"{merge_path}/{merge_folder}")

    # make sub directories
    os.mkdir(f"{merge_path}/{merge_folder}/src")
    os.mkdir(f"{merge_path}/{merge_folder}/input")
    os.mkdir(f"{merge_path}/{merge_folder}/output")
    os.mkdir(f"{merge_path}/{merge_folder}/note")

    # link setup files
    for file in ["setup.py", "general_utils.py", "match_functions.py", "merge_functions.py", "update_functions.py"]:
        os.symlink(f"{os.path.abspath(share_path)}/src/{file}", f"{merge_path}/{merge_folder}/src/{file}") # use absolute path/relative doesn't preserve

    # link previous merge officer reference from max merge number
    os.symlink(f"{os.path.abspath(merge_path)}/{max_merge_folder}/output/officer-reference.csv.gz", f"{merge_path}/{merge_folder}/input/officer-reference.csv.gz")

    # link profiles and base file from individual
    os.symlink(f"{os.path.abspath(individual_path)}/{foia_name}/export/output/{foia_name}.csv.gz", f"{merge_path}/{merge_folder}/input/{foia_name}.csv.gz")
    os.symlink(f"{os.path.abspath(individual_path)}/{foia_name}/export/output/{foia_name}_profiles.csv.gz", f"{merge_path}/{merge_folder}/input/{foia_name}_profiles.csv.gz")

    template_vars = {
        "merge_number": new_merge_number,
        "foia_name": foia_name,
        "author": author}

    # render template
    render_template_to_path(template_name="merge.py.mako", path=f"{merge_path}/{merge_folder}/src/merge.py", template_vars=template_vars)
    render_notebook_for_task("merge", f"{merge_path}/{merge_folder}", template_vars)
    return
    
def add_to_resolve(root_path, foia_type, foia_name, profile):
    pass

def get_column_names(file, sheet=""):
    if file.endswith(".csv"):
        return pd.read_csv(file, encoding='latin-1', header=0, nrows=1).columns
    else:
        if sheet:
            return pd.read_excel(file, sheet_name=sheet, header=0, nrows=1).columns
        else:
            return pd.read_excel(file, header=0, nrows=1).columns
        
if __name__ == '__main__':
    cpdp()