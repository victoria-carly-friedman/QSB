# importing the required packages
import io
import os
import re
import sys
import time
import pytz
import glob
import string
import shutil

from datetime import datetime, timedelta

import pandas as pd
import numpy  as np
#---------------------------------------------------------------------------------------------------------------------------
# pandas display options
pd.set_option('display.max_rows',None)
pd.set_option('display.max_columns',None)
pd.set_option('display.max_colwidth',1000)
pd.set_option('display.width',1000)
#---------------------------------------------------------------------------------------------------------------------------
# helper functions
def display(message,value):
    if isinstance(value,int):
        value = format(value,",d")
    else:
        try:
            value = str(value)
        except:
            value = "error"   
    return "{0: <40}".format(message)+" : "+value 
#---------------------------------------------------------------------------------------------------------------------------
def line():
    return "----------------------------------------------------------------------------------------"
#---------------------------------------------------------------------------------------------------------------------------
def get_datetime():
    return datetime.strftime(datetime.now(tz=pytz.timezone("Asia/Kolkata")),format="%Y-%m-%d %I:%M %p")
#---------------------------------------------------------------------------------------------------------------------------
def generate_id(number_size=4,alphabet_size=3):
    
    alphabet_uppercase = list(string.ascii_uppercase)
    numbers            = [*range(0,10)]

    id_part_1  = "".join(np.random.choice(alphabet_uppercase,size=alphabet_size,replace=True,p=None))

    id_part_2  = "".join([str(i) for i in np.random.choice(numbers,size=number_size,replace=True,p=None)])

    return id_part_1 + id_part_2
#---------------------------------------------------------------------------------------------------------------------------
def get_id(existing_ids):
    
    new_id     = generate_id()
    
    while new_id in existing_ids:
        
        new_id = generate_id()
    
    return new_id
#---------------------------------------------------------------------------------------------------------------------------
def get_pick_id(path):
    
    return pd.concat([pd.read_excel(file_path) for file_path in glob.glob(path+"\*.xlsx")],axis=0)
#---------------------------------------------------------------------------------------------------------------------------
def get_auto_id(path):
    
    return pd.read_csv(path)
#---------------------------------------------------------------------------------------------------------------------------
def update_auto_id(id):
    
    auto_id_df = get_auto_id(auto_id_path)

    auto_id    = set(auto_id_df["id"].tolist())

    new_id     = set(id)

    check      = list(new_id - auto_id)

    if len(check) == 0:

        return "ids already exists in auto_id"

    else:
        update_df = pd.DataFrame({"id":check})

        update_df.to_csv(auto_id_path,mode="a",index=False,header=None)

        return f"{update_df.shape[0]} ids are successfully updated"
#---------------------------------------------------------------------------------------------------------------------------
def validate_ticket(ticket):
    
    if len(ticket) == 7:
        
        alphabet = ticket[:3]
        
        number   = ticket[3:]
        
        if alphabet.isalpha() and number.isnumeric():
            
            return ticket
        
        else:
            
            return ticket+" "+"not valid"
    else:
        
            return ticket+" "+"not valid"
#---------------------------------------------------------------------------------------------------------------------------
def make_data(path):
    
    other_formats = [i for i in os.listdir(path) if ".jpg" not in i]

    if not other_formats:

        make_data = []

        for i in os.listdir(path):

            extract_pattern    = re.findall(r'\{.*?\}',i)

            if "+" in i and len(extract_pattern) > 0:
                
                extract_ticket = extract_pattern[0].replace("{","").replace("}","").replace(".jpg","")

                make_data.append({"filename":i.split("+")[0],"ticket":validate_ticket(extract_ticket)})

            else:

                make_data.append({"filename":i.replace(".jpg",""),"ticket":None})
        
        return pd.DataFrame(make_data)

    else:

        print("files have formats other than .jpg")
        print(display("number of files that are not in .jpg format",len(other_formats)))
        print(other_formats)
        sys.exit()
#---------------------------------------------------------------------------------------------------------------------------
def folder_contents(df,folder_name,assumed_folder):
    
    pornstar_folder = [i[0].strip() for i in df["filename"].str.split("-").tolist()]
    
    if len(list(set(pornstar_folder))) > 1:
        
        print("the "+folder_name+" has more than one pornstar")
        
        print(folder_name+" folder can have only one pornstar")
        
        print_number     = 0
        
        for i in list(set(pornstar_folder)):
            
            print(str(print_number+1)+". "+i)
            
            print_number = print_number + 1 
        
        return sys.exit()    
    
    else:
        
        if pornstar_folder[0] == assumed_folder:
            
            return pornstar_folder[0]
        
        else:
            
            print(display("pornstar in the folder",pornstar_folder[0]))
            print(display("processing for pornstar",assumed_folder))
                  
            return sys.exit()
#---------------------------------------------------------------------------------------------------------------------------    
india_datetime  = datetime.strftime(datetime.now(tz=pytz.timezone("Asia/Kolkata")),format="%Y-%m-%d %I:%M %p")
source_path     = input("source path : ")

pornstar_folder = source_path.split("\\")[-1]

print(line())
print(display("code execution time",india_datetime))
print(display("current working directory",os.getcwd()))

os.chdir(source_path)

print(display("changing directory to",os.getcwd()))

tickets_path  = source_path.replace("Source","Tickets")+" 🎫"

# pick_id_path  = "G:\\QSB (dev)\\ID (dev)\\ID (pick) (dev)"
# auto_id_path  = "G:\\QSB (dev)\\ID (dev)\\ID (auto) (dev)\\tapes_ids (auto).csv"

pick_id_path  = "G:\\Queen's Blood\\ID\\ID (pick)"
auto_id_path  = "G:\\Queen's Blood\\ID\\ID (auto)\\tapes_ids (auto).csv"

print(display("source  path",source_path))
print(display("tickets path",tickets_path))
print(display("pick id path",pick_id_path))
print(display("auto id pack",auto_id_path))

print(line())
#---------------------------------------------------------------------------------------------------------------------------
# source_df
print(line())
print("source details")

source_df = make_data(source_path)

if source_df.empty:

    print("source_df is empty")
    sys.exit()

else:

    source_df["filename"]  = source_df["filename"].astype("str")

    source_df["filename"]  = source_df["filename"].str.strip()

    source_folder          = folder_contents(source_df,"source folder",pornstar_folder)
    
    print(display("source folder",source_folder))

    if  source_df["ticket"].isnull().sum() != len(source_df):
        
        source_df_not_null = source_df[source_df["ticket"].notnull()]
                  
        invalid_source_df  = source_df_not_null[source_df_not_null["ticket"].str.contains("not valid")]

        if len(invalid_source_df) > 0:
            
            print("source_df has invalid tickets")
            print(display("number of invalid tickets",len(invalid_source_df)))
            print(invalid_source_df)
            sys.exit()
        else:
            
            print("source_df has valid tickets")

    else:
        
        print("source_df has no tickets")

print(line())
#---------------------------------------------------------------------------------------------------------------------------
# tickets_df
print(line())
print("tickets details")

tickets_df = make_data(tickets_path)

if tickets_df.empty:

    print("tickets_df is empty")

    tickets_df     = pd.DataFrame({"filename":[],"ticket":[]})

    tickets_folder = pornstar_folder

else:

    tickets_df["filename"] = tickets_df["filename"].astype("str")
    
    tickets_df["filename"] = tickets_df["filename"].str.strip()

    tickets_folder         = folder_contents(tickets_df,"tickets folder",pornstar_folder)
    
    print(display("tickets folder",tickets_folder))

    tickets_df_not_null    = tickets_df[tickets_df["ticket"].notnull()]

    invalid_tickets_df     = tickets_df_not_null[tickets_df_not_null["ticket"].str.contains("not valid")]

    if len(invalid_tickets_df) > 0:
        
        print("tickets_df has invalid tickets")
        print(display("number of invalid tickets",len(invalid_tickets_df)))
        print(invalid_tickets_df)
        sys.exit()

    else:
        
        print("tickets_df have valid tickets")

    if tickets_df["ticket"].isnull().sum() > 0:
        
        print("tickets_df have nulls in the ticket column")
        print(display("number of nulls",tickets_df["ticket"].isnull().sum()))
        print(tickets_df[tickets_df["ticket"].isnull()])
        sys.exit()

    else:
        
        print("tickets_df has no nulls in the ticket column")

print(line())
#---------------------------------------------------------------------------------------------------------------------------
print("info")

if source_folder == tickets_folder:

    qsb = source_df.merge(tickets_df,on="filename",how="left",suffixes=(' (source_df)', ' (tickets_df)'))

    qsb = qsb.replace({np.nan:None})

    print(qsb)

else:

    print(display("source_folder",source_folder))
    print(display("tickets_folder",tickets_folder))

    print("source_folder &  tickets_folder should be matching")

    sys.exit()
#---------------------------------------------------------------------------------------------------------------------------
# remove_ticket_from_source
print(line())
print("remove_ticket_from_source")

remove_ticket_from_source =  qsb[qsb["ticket (source_df)"]==qsb["ticket (tickets_df)"]]

if len(remove_ticket_from_source) == 0:
    
    print("there are no tickets to remove from source")
    
else:
    
    tickets_removed     = 0 

    for index,row in remove_ticket_from_source.iterrows():

        to_replace      = row["filename"]+" + "+"{"+row["ticket (source_df)"]+"}"+".jpg"

        replace_with    = row["filename"]+".jpg"

        os.rename(to_replace,replace_with)
        
        tickets_removed = tickets_removed + 1
    
    print(remove_ticket_from_source.sort_values(by="filename",ascending=True))
    
    print(display("tickets removed from source",tickets_removed))

print(line())
#---------------------------------------------------------------------------------------------------------------------------
# add_new_tickets
print(line())
print("add_new_tickets")

add_new_tickets = qsb[(qsb["ticket (source_df)"].isnull())&(qsb["ticket (tickets_df)"].isnull())]

if len(add_new_tickets) == 0:
    
    print("no new tickets to add")

else:
    
    newly_added_ids   = []

    pick_id           = list(get_pick_id(pick_id_path)["id"]) 

    for i in list(add_new_tickets["filename"]):
        
        to_replace    = i+".jpg"
        
        create_id     = get_id(pick_id + list(get_auto_id(auto_id_path)["id"]))
        
        replace_with  = i+" + {"+ create_id + "}.jpg"
        
        newly_added_ids.append(create_id)
        
        os.rename(to_replace,replace_with)
        
    print(add_new_tickets)
    
    print(display("new tickets added",len(newly_added_ids)))
    
    print(update_auto_id(newly_added_ids))

print(line())
#---------------------------------------------------------------------------------------------------------------------------




