import numpy as np
import functools
import random
import pandas as pd

from corrupt.geco_corrupt import CorruptValueQuerty, position_mod_uniform


@functools.lru_cache(maxsize=None)
def get_given_name_alternatives_lookup():
    in_path = "out_data/wikidata/processed/alt_name_lookups/given_name_lookup.parquet"
    df = pd.read_parquet(in_path).set_index("original_name")
    return df.to_dict(orient="index")


@functools.lru_cache(maxsize=None)
def get_family_name_alternatives_lookup():
    in_path = "out_data/wikidata/processed/alt_name_lookups/family_name_lookup.parquet"
    df = pd.read_parquet(in_path).set_index("original_name")
    return df.to_dict(orient="index")


######################################
# FOR ALSPAC 
######################################

def alspac_G1_first_name_gen_uncorrupted_record(formatted_master_record, record_to_modify={}):
    record_to_modify["G1_firstname"] = formatted_master_record["G1_firstname"] 
    return record_to_modify

def alspac_G1_surname_gen_uncorrupted_record(formatted_master_record, record_to_modify={}):
    record_to_modify["G1_surname"] = formatted_master_record["G1_surname"] 
    return record_to_modify

def alspac_G0_surname_gen_uncorrupted_record(formatted_master_record, record_to_modify={}):
    record_to_modify["G0_surname"] = formatted_master_record["G0_surname"] 
    return record_to_modify

# Random first name (Random non-diminutive first name)
def alspac_first_name_random(formatted_master_record, record_to_modify={}):
    
    orig_firstname = formatted_master_record['G1_firstname']
    first_name_alt_lookup = get_given_name_alternatives_lookup()
    new_firstname = random.choice(list(first_name_alt_lookup.keys()))
    while new_firstname == orig_firstname:
        new_firstname = random.choice(list(first_name_alt_lookup.keys()))
        if new_firstname != orig_firstname:
            continue

    if orig_firstname is None:
        record_to_modify["G1_firstname"] = None
        return record_to_modify

    record_to_modify["G1_firstname"] = " ".join(str(new_firstname)).lower()

    return record_to_modify


#random G1 last name - married/devorced

def alspac_G1_surname_random(formatted_master_record, record_to_modify={}):
    
    orig_surname = formatted_master_record['G1_surname']
    family_name_alt_lookup = get_family_name_alternatives_lookup()
    new_surname = random.choice(list(family_name_alt_lookup.keys()))
    while new_surname == orig_surname:
        new_surname = random.choice(list(family_name_alt_lookup.keys()))
        if new_surname != orig_surname:
            continue

    if orig_surname is None:
        record_to_modify["G1_surname"] = None
        return record_to_modify

    record_to_modify["G1_surname"] = " ".join(str(new_surname)).lower()

    return record_to_modify

#random G0 last name - married/devorced
def alspac_G0_surname_random(formatted_master_record, record_to_modify={}):
    
    orig_surname = formatted_master_record['G0_surname']
    family_name_alt_lookup = get_family_name_alternatives_lookup()
    new_surname = random.choice(list(family_name_alt_lookup.keys()))

    while new_surname == orig_surname:
        new_surname = random.choice(list(family_name_alt_lookup.keys()))
        if new_surname != orig_surname:
            continue

    if orig_surname is None:
        record_to_modify["G0_surname"] = None
        return record_to_modify

    record_to_modify["G0_surname"] = " ".join(str(new_surname)).lower()

    return record_to_modify

#alspac alternative first names

def alspac_first_name_alternatives(formatted_master_record, record_to_modify={}):
    """ choose alternative first names"""
    
    given = formatted_master_record["G1_firstname"]
    
    if given is None:
        record_to_modify["G1_firstname"] = None
        return record_to_modify
    

    given_name_alt_lookup = get_given_name_alternatives_lookup()
    
    output_names = []
    if given in given_name_alt_lookup:
        name_dict = given_name_alt_lookup[given]
        alt_names = name_dict["alt_name_arr"]
        weights = name_dict["alt_name_weight_arr"]
        output_names.append(np.random.choice(alt_names, p=weights))
    else:
        output_names.append(given)

    record_to_modify["G1_firstname"] = " ".join(str(output_names)).lower()

    return record_to_modify

def alspac_first_name_insertion(formatted_master_record, record_to_modify):
    """insertion of extra term in first name"""
    given = str(formatted_master_record['G1_firstname'])
    first_name_alt_lookup = get_given_name_alternatives_lookup()
    new_firstname = str(random.choice(list(first_name_alt_lookup.keys())))

    if given is None or given == "":
       record_to_modify["G1_firstname"] = new_firstname
    else:
        record_to_modify["G1_firstname"] = given + " " + new_firstname

    return record_to_modify

def alspac_first_name_deletion(formatted_master_record, record_to_modify):
    orig_firstname = str(formatted_master_record['G1_firstname'])
    """deletion of extra term in first name"""
    num_of_terms = str(orig_firstname).count(" ") + 1 
    if num_of_terms == 0 or num_of_terms == 1:
      record_to_modify["G1_firstname"] = orig_firstname 
        
    
    # new_name = new_name.split("-") # remove hyphens in first name
    new_name = orig_firstname.split(" ")
    if num_of_terms == 2:
        first_term = new_name.pop(0)
        second_term = new_name
    elif num_of_terms == 3:
        first_term = new_name.pop(0)
        second_term = new_name.pop(0)
        third_term = new_name.pop(0)
    elif num_of_terms >= 4:
        first_term = new_name.pop(0)
        second_term = new_name.pop(0)
        third_term = new_name.pop(0)
        forth_term = new_name.pop(0)


    # count number of terms, and condition on it
    if num_of_terms >= 4:
        if random.randint(1,4)==1:
            new_name = second_term.lower() + " " + third_term.lower() + " " + forth_term.lower()
            record_to_modify["G1_firstname"] = new_name
        elif random.randint(1,4)==2:
            new_name = first_term.lower() + " " + third_term.lower() + " " + forth_term.lower()
            record_to_modify["G1_firstname"] = new_name
        elif random.randint(1,4)==3:
            new_name = first_term.lower() + " " + second_term.lower() + " " + forth_term.lower()
            record_to_modify["G1_firstname"] = new_name
        elif random.randint(1,4)==4:
            new_name = first_term.lower() + " " + second_term.lower() + " " + third_term.lower()
            record_to_modify["G1_firstname"] = new_name
    elif num_of_terms == 3:
        if random.randint(1,3)==1:
            new_name = second_term.lower() + " " + third_term.lower()
            record_to_modify["G1_firstname"] = new_name
        elif random.randint(1,3)==2:
            new_name = first_term.lower() + " " + third_term.lower()
            record_to_modify["G1_firstname"] = new_name
        elif random.randint(1,3)==3:
            new_name = second_term.lower() + " " + third_term.lower()
            record_to_modify["G1_firstname"] = new_name
    elif num_of_terms == 2:
        if random.randint(1,2)==1:
            new_name = first_term
            record_to_modify["G1_firstname"] = new_name
        elif random.randint(1,2)==2:
            new_name = second_term
            record_to_modify["G1_firstname"] = new_name

    return record_to_modify


def alspac_G1_last_name_insertion(formatted_master_record, record_to_modify):
    """insert extra term in surname"""
    
    options = str(formatted_master_record['G1_surname'])
    
    lastname_orig = options
    family_name_alt_lookup = get_family_name_alternatives_lookup()
    new_surname = str(random.choice(list(family_name_alt_lookup.keys())))
    
    if options is None or options == "":
        record_to_modify["G1_surname"] = new_surname
    else:
        record_to_modify["G1_surname"] = lastname_orig.lower() + " " + new_surname.lower()
    return record_to_modify


def alspac_G1_last_name_deletion(formatted_master_record, record_to_modify):
    """deletion of extra term in surname"""

    orig_lastname = str(formatted_master_record['G1_surname'])
    num_of_terms = orig_lastname.count(" ") + 1
    if num_of_terms == 0 or num_of_terms == 1:
      record_to_modify["G1_surname"] = orig_lastname 
        
    
    # new_name = new_name.split("-") # remove hyphens in first name
    new_name = orig_lastname.split(" ")
    if num_of_terms == 2:
        first_term = new_name.pop(0)
        second_term = new_name
    elif num_of_terms == 3:
        first_term = new_name.pop(0)
        second_term = new_name.pop(0)
        third_term = new_name.pop(0)
    elif num_of_terms >= 4:
        first_term = new_name.pop(0)
        second_term = new_name.pop(0)
        third_term = new_name.pop(0)
        forth_term = new_name.pop(0)


    # count number of terms, and condition on it
    if num_of_terms >= 4:
        if random.randint(1,4)==1:
            new_name = second_term.lower() + " " + third_term.lower() + " " + forth_term.lower()
            record_to_modify["G1_surname"] = new_name
        elif random.randint(1,4)==2:
            new_name = first_term.lower() + " " + third_term.lower() + " " + forth_term.lower()
            record_to_modify["G1_surname"] = new_name
        elif random.randint(1,4)==3:
            new_name = first_term.lower() + " " + second_term.lower() + " " + forth_term.lower()
            record_to_modify["G1_surname"] = new_name
        elif random.randint(1,4)==4:
            new_name = first_term.lower() + " " + second_term.lower() + " " + third_term.lower()
            record_to_modify["G1_surname"] = new_name
    elif num_of_terms == 3:
        if random.randint(1,3)==1:
            new_name = second_term.lower() + " " + third_term.lower()
            record_to_modify["G1_surname"] = new_name
        elif random.randint(1,3)==2:
            new_name = first_term.lower() + " " + third_term.lower()
            record_to_modify["G1_surname"] = new_name
        elif random.randint(1,3)==3:
            new_name = first_term.lower() + " " + second_term.lower()
            record_to_modify["G1_surname"] = new_name
    elif num_of_terms == 2:
        if random.randint(1,2)==1:
            new_name = first_term
            record_to_modify["G1_surname"] = new_name
        elif random.randint(1,2)==2:
            new_name = second_term
            record_to_modify["G1_surname"] = new_name

    return record_to_modify

def alspac_G0_last_name_insertion(formatted_master_record, record_to_modify):
    """insert extra term in surname"""
    
    options = str(formatted_master_record["G0_surname"])

    
    lastname_orig = options
    family_name_alt_lookup = get_family_name_alternatives_lookup()
    new_surname = str(random.choice(list(family_name_alt_lookup.keys())))
    
    if options is None or options == "":
        record_to_modify["G0_surname"] = new_surname
    else:
        record_to_modify["G0_surname"] = lastname_orig.lower() + " " + new_surname.lower()                                              
    return record_to_modify


def alspac_G0_last_name_deletion(formatted_master_record, record_to_modify):
    """deletion of extra term in surname"""

    orig_lastname = str(formatted_master_record['G0_surname'])
    num_of_terms = orig_lastname.count(" ") + 1
    if num_of_terms == 0 or num_of_terms == 1:
      record_to_modify["G0_surname"] = orig_lastname 
        
    
    # new_name = new_name.split("-") # remove hyphens in first name
    new_name = orig_lastname.split(" ")
    if num_of_terms == 2:
        first_term = new_name.pop(0)
        second_term = new_name
    elif num_of_terms == 3:
        first_term = new_name.pop(0)
        second_term = new_name.pop(0)
        third_term = new_name.pop(0)
    elif num_of_terms >= 4:
        first_term = new_name.pop(0)
        second_term = new_name.pop(0)
        third_term = new_name.pop(0)
        forth_term = new_name.pop(0)


    # count number of terms, and condition on it
    if num_of_terms >= 4:
        if random.randint(1,4)==1:
            new_name = second_term.lower() + " " + third_term.lower() + " " + forth_term.lower()
            record_to_modify["G0_surname"] = new_name
        elif random.randint(1,4)==2:
            new_name = first_term.lower() + " " + third_term.lower() + " " + forth_term.lower()
            record_to_modify["G0_surname"] = new_name
        elif random.randint(1,4)==3:
            new_name = first_term.lower() + " " + second_term.lower() + " " +  forth_term.lower()
            record_to_modify["G0_surname"] = new_name
        elif random.randint(1,4)==4:
            new_name = first_term.lower() + " " + second_term.lower() + " " + third_term.lower() 
            record_to_modify["G0_surname"] = new_name
    elif num_of_terms == 3:
        if random.randint(1,3)==1:
            new_name = second_term.lower() + " " + third_term.lower()
            record_to_modify["G0_surname"] = new_name
        elif random.randint(1,3)==2:
            new_name = first_term.lower() + " " + third_term.lower()
            record_to_modify["G0_surname"] = new_name
        elif random.randint(1,3)==3:
            new_name = first_term.lower() + " " + second_term.lower()
            record_to_modify["G0_surname"] = new_name
    elif num_of_terms == 2:
        if random.randint(1,2)==1:
            new_name = first_term
            record_to_modify["G0_surname"] = new_name
        elif random.randint(1,2)==2:
            new_name = second_term
            record_to_modify["G0_surname"] = new_name

    return record_to_modify


def alspac_first_name_typo(formatted_master_record, record_to_modify={}):

    options = str(formatted_master_record["G1_firstname"])

    if options is None or options == "":
        record_to_modify["G1_firstname"] = None
        return record_to_modify

    first_name = options

    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    record_to_modify["G1_firstname"] = querty_corruptor.corrupt_value(first_name)

    return record_to_modify

def alspac_G1_last_name_typo(formatted_master_record, record_to_modify={}):

    options = str(formatted_master_record["G1_surname"])

    if options is None or options == "":
        record_to_modify["G1_surname"] = None
        return record_to_modify

    last_name = options

    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    record_to_modify["G1_surname"] = querty_corruptor.corrupt_value(last_name)

    return record_to_modify

def alspac_G0_last_name_typo(formatted_master_record, record_to_modify={}):

    options = str(formatted_master_record["G0_surname"])

    if options is None or options == "":
        record_to_modify["G0_surname"] = None
        return record_to_modify

    last_name = options

    querty_corruptor = CorruptValueQuerty(
        position_function=position_mod_uniform, row_prob=0.5, col_prob=0.5
    )

    record_to_modify["G0_surname"] = querty_corruptor.corrupt_value(last_name)

    return record_to_modify

def alspac_name_inversion(formatted_master_record, record_to_modify):

    given = str(formatted_master_record["G1_firstname"])
    family = str(formatted_master_record["G1_surname"])

    if len(given) > 0 and len(family) > 0:
       record_to_modify["G1_firstname"] = family
       record_to_modify["G1_surname"] = given
    return record_to_modify
