import argparse
from asyncio import base_events
from email.mime import base
from pathlib import Path
import os
import logging
import pandas as pd
import numpy as np
import duckdb

# create path and transform .csv gold to parquet
df = pd.read_csv('ALSPAC_syn_gold.csv')
df.to_parquet('syn_alspac_to_corrupt.parquet')
pd.read_parquet('syn_alspac_to_corrupt.parquet', engine='pyarrow')


# This Block Introduces the corruption functions 

from corrupt.corruption_functions import (
    master_record_no_op,
    alspac_generate_uncorrupted_output_record,
    null_corruption,
    format_master_data,
)

from corrupt.corrupt_name import (
    alspac_G1_first_name_gen_uncorrupted_record,
    alspac_G1_surname_gen_uncorrupted_record,
    alspac_G0_surname_gen_uncorrupted_record,
    alspac_first_name_random, 
    alspac_G1_surname_random, 
    alspac_G0_surname_random, 
    alspac_first_name_alternatives, 
    alspac_first_name_insertion,
    alspac_first_name_deletion,
    alspac_G1_last_name_insertion,
    alspac_G1_last_name_deletion,
    alspac_G0_last_name_insertion,
    alspac_G0_last_name_deletion,
    alspac_first_name_typo, 
    alspac_name_inversion,
)

from corrupt.corrupt_id import (
    gen_uncorrupted_id
)

from corrupt.corrupt_date import (
    gen_uncorrupted_date_alspac,
    date_corrupt_jan_first,
)

from corrupt.corrupt_matcat import (
    gen_uncorrupted_matcat,
)

from corrupt.corrupt_ethgroup import (
    gen_uncorrupted_ethgroup,
)

from corrupt.corrupt_gender import (
    gen_uncorrupted_gender,
)

from corrupt.corrupt_imd import (
    gen_uncorrupted_imd,
)
# Change File Paths: add new folder and file.

output = "output"

from datetime import date
date = str(date.today())
ALSPAC_corrupt_outpath = os.path.join(output,date) #where to deposit the corrupted data

from corrupt.record_corruptor import (
    ProbabilityAdjustmentFromLookup,
    RecordCorruptor,
    CompositeCorruption,
    ProbabilityAdjustmentFromSQL,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(message)s",
)
logger.setLevel(logging.INFO)


con = duckdb.connect()

# change path to gold.

in_path = os.path.join("syn_alspac_to_corrupt.parquet")


# Configure how corruptions will be made for each field

# Col name is the OUTPUT column name.  For instance, we may input given name,
# family name etc to output full_name

# Guide to keys:
# format_master_data.  This function may apply additional cleaning to the master
# record.  The same formatted master data is then available to the
# 'gen_uncorrupted_record' and 'corruption_functions'

from functools import partial
from corrupt.geco_corrupt import get_zipf_dist

config = [
    {
        "col_name": "random_id",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_id,
    },
    {
        "col_name": "maternal_agecat",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_matcat,
    },
    {
        "col_name": "ethgroup",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_ethgroup,
    },
    {
        "col_name": "gender",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_gender,
    },
    {
        "col_name": "imddecile",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_imd,
    },
    {
        "col_name": "g1_dob",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_date_alspac,
    },
    {
        "col_name": "G0_surname",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": alspac_G0_surname_gen_uncorrupted_record,
    },
    {
        "col_name": "G1_surname",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": alspac_G1_surname_gen_uncorrupted_record,
    },
        {
        "col_name": "G1_firstname",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": alspac_G1_first_name_gen_uncorrupted_record,
    },
]


rc = RecordCorruptor()

########
# Date of birth
########
"""
g1_dob_jan_first = CompositeCorruption(
    name="g1_dob_jan_first", baseline_probability=0.005
)

g1_dob_jan_first.add_corruption_function(
    date_corrupt_jan_first, args ={"input_colname": "g1_dob","output_colname": "g1_dob"}
)

rc.add_composite_corruption(g1_dob_jan_first)

sql_condition = "year(try_cast(g1_dob as date)) < 1900"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_dob_jan_first, 4)
rc.add_probability_adjustment(adjustment)

rc.add_simple_corruption(
    name="dob_null",
    corruption_function=null_corruption,
    args={"output_colname": "g1_dob"},
    baseline_probability=0.01,
)
"""
########
# Name-based corruptions
########

## First Name Errors
firstname_random_corruption = CompositeCorruption(name="random_first", baseline_probability=0.15)
firstname_random_corruption.add_corruption_function(alspac_first_name_random, args={})
rc.add_composite_corruption(firstname_random_corruption)


firstname_variant_corruption = CompositeCorruption(name="first_name_variants", baseline_probability=0.25)
firstname_variant_corruption.add_corruption_function(alspac_first_name_alternatives, args={})
rc.add_composite_corruption(firstname_variant_corruption)

firstname_deletion_corruption = CompositeCorruption(name="first_name_deletion",baseline_probability=0.1)
firstname_deletion_corruption.add_corruption_function(alspac_first_name_deletion, args={})
rc.add_composite_corruption(firstname_deletion_corruption)


firstname_insertion_corruption = CompositeCorruption(name="first_name_insertion",baseline_probability=0.1)
firstname_insertion_corruption.add_corruption_function(alspac_first_name_insertion,args={})
rc.add_composite_corruption(firstname_insertion_corruption)

firstname_typo_corruption = CompositeCorruption(name="first_name_typo",baseline_probability=0.1)
firstname_typo_corruption.add_corruption_function(alspac_first_name_typo,args={})
rc.add_composite_corruption(firstname_typo_corruption)


# Ethnicity adjustments 
# white as reference for first name errors
sql_condition = "ethgroup in ('Missing', 'NA','None')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 0.72)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 0.72)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 0.72)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 0.72)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 0.72)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Black')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 1.19)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 1.19)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 1.19)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 1.19)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 1.19)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Asian')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 0.99)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 0.99)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 0.99)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 0.99)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 0.99)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Other')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 1.13)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 1.13)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 1.13)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 1.13)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 1.13)
rc.add_probability_adjustment(adjustment)

# Maternal age adjustments 
# 30-39 as reference for first name errors
sql_condition = "maternal_agecat in ('<20')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 0.57)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 0.57)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 0.57)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 0.57)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 0.57)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('20-29')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 0.85)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 0.85)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 0.85)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 0.85)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 0.85)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('40+')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 1.09)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 1.09)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 1.09)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 1.09)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 1.09)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('Missing', 'NA','None')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_random_corruption, 1.24)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_variant_corruption, 1.24)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_deletion_corruption, 1.24)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_insertion_corruption, 1.24)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, firstname_typo_corruption, 1.24)
rc.add_probability_adjustment(adjustment)


## G0 Last Name Errors
g0_lastname_deletion_corruption = CompositeCorruption(name="G0_last_name_deletion",baseline_probability=0.1)
g0_lastname_deletion_corruption.add_corruption_function(alspac_G0_last_name_deletion,args={})
rc.add_composite_corruption(g0_lastname_deletion_corruption)

g0_lastname_insertion_corruption = CompositeCorruption(name="G0_last_name_insertion",baseline_probability=0.1)
g0_lastname_insertion_corruption.add_corruption_function(alspac_G0_last_name_insertion,args={})
rc.add_composite_corruption(g0_lastname_insertion_corruption)

g0_lastname_random_corruption = CompositeCorruption(name="G0_last_name_random",baseline_probability=0.2)
g0_lastname_random_corruption.add_corruption_function(alspac_G0_surname_random,args={})
rc.add_composite_corruption(g0_lastname_random_corruption)

# Ethnicity
sql_condition = "ethgroup in ('Missing', 'NA','None')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 0.89)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 0.89)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 0.89)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Black')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 1.05)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 1.05)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 1.05)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Asian')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 0.46)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 0.46)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 0.46)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Other')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 1.08)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 1.08)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 1.08)
rc.add_probability_adjustment(adjustment)

# Maternal Age Cat
sql_condition = "maternal_agecat in ('<20')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 2.89)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 2.89)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 2.89)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('20-29')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 1.61)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 1.61)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 1.61)
rc.add_probability_adjustment(adjustment)


sql_condition = "maternal_agecat in ('40+')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 1.14)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 1.14)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 1.14)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('Missing', 'NA','None')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_deletion_corruption, 1.50)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_insertion_corruption, 1.50)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g0_lastname_random_corruption, 1.50)
rc.add_probability_adjustment(adjustment)


#G1 Last name
g1_lastname_deletion_corruption = CompositeCorruption(name="G1_last_name_deletion",baseline_probability=0.1)
g1_lastname_deletion_corruption.add_corruption_function(alspac_G1_last_name_deletion,args={})
rc.add_composite_corruption(g1_lastname_deletion_corruption)

g1_lastname_insertion_corruption = CompositeCorruption(name="G1_last_name_insertion",baseline_probability=0.1)
g1_lastname_insertion_corruption.add_corruption_function(alspac_G1_last_name_insertion,args={})
rc.add_composite_corruption(g1_lastname_insertion_corruption)

g1_lastname_random_corruption = CompositeCorruption(name="G1_last_name_random",baseline_probability=0.2)
g1_lastname_random_corruption.add_corruption_function(alspac_G1_surname_random,args={})
rc.add_composite_corruption(g1_lastname_random_corruption)

# Ethnicity
sql_condition = "ethgroup in ('Missing', 'NA','None')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 0.77)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 0.77)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 0.77)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Black')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 1.43)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 1.43)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 1.43)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Asian')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 0.34)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 0.34)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 0.34)
rc.add_probability_adjustment(adjustment)

sql_condition = "ethgroup in ('Other')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 2.05)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 2.05)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 2.05)
rc.add_probability_adjustment(adjustment)

# Maternal Age Cat
sql_condition = "maternal_agecat in ('<20')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 2.60)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 2.60)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 2.60)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('20-29')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 1.43)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 1.43)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 1.43)
rc.add_probability_adjustment(adjustment)


sql_condition = "maternal_agecat in ('40+')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 1.44)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 1.44)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 1.44)
rc.add_probability_adjustment(adjustment)

sql_condition = "maternal_agecat in ('Missing', 'NA','None')"
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_deletion_corruption, 2.13)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_insertion_corruption, 2.13)
rc.add_probability_adjustment(adjustment)
adjustment = ProbabilityAdjustmentFromSQL(sql_condition, g1_lastname_random_corruption, 2.13)
rc.add_probability_adjustment(adjustment)

name_inversion_corruption = CompositeCorruption(
    name = "alspac_name_inversion", baseline_probability= 0.05
)
name_inversion_corruption.add_corruption_function(alspac_name_inversion, args={})
rc.add_composite_corruption(name_inversion_corruption)

adjustment_lookup_eth = {
    "ethgroup":{
        "White":[(name_inversion_corruption, 1)],
        "Black":[(name_inversion_corruption, 1.19)],
        "Other":[(name_inversion_corruption, 1.13)],
        "Asian":[(name_inversion_corruption, 0.99)],
    }
}

adjustment_eth = ProbabilityAdjustmentFromLookup(adjustment_lookup_eth)
rc.add_probability_adjustment(adjustment_eth)

adjustment_lookup_mat = {
    "maternal_agecat":{
        "<20":[(name_inversion_corruption, 2.60)],
        "20-29":[(name_inversion_corruption, 1.43)],
        "30-39":[(name_inversion_corruption, 1)],
        "40+":[(name_inversion_corruption, 1.44)],
        "NA":[(name_inversion_corruption, 2.13)],
    }
}

adjustment_mat = ProbabilityAdjustmentFromLookup(adjustment_lookup_mat)
rc.add_probability_adjustment(adjustment_mat)
                   
max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)


pd.options.display.max_columns = 1000
pd.options.display.max_colwidth = 1000

Path(ALSPAC_corrupt_outpath).mkdir(parents=True, exist_ok=True)

out_path = os.path.join(ALSPAC_corrupt_outpath)

sql = f"""
select *
from '{in_path}'
"""

raw_data = con.execute(sql).df()
records = raw_data.to_dict(orient="records")

output_records = []
for i, master_input_record in enumerate(records):

    # Formats the input data into an easy format for producing
    # an uncorrupted/corrupted outputs records
    formatted_master_record = format_master_data(master_input_record, config)

    uncorrupted_output_record = alspac_generate_uncorrupted_output_record(
        formatted_master_record, config
    )
    uncorrupted_output_record["corruptions_applied"] = []

    output_records.append(uncorrupted_output_record)

    # How many corrupted records to generate
    total_num_corrupted_records = np.random.choice(
        zipf_dist["vals"], p=zipf_dist["weights"]
    )

    for k in range(total_num_corrupted_records):
        record_to_modify = uncorrupted_output_record.copy()
        record_to_modify["corruptions_applied"] = []
        record_to_modify["uncorrupted_record"] = False
        rc.apply_probability_adjustments(uncorrupted_output_record)
        corrupted_record = rc.apply_corruptions_to_record(
            formatted_master_record,
            record_to_modify,
        )
        output_records.append(corrupted_record)
        print("write record number " + str(k) + "for id number " + str(i))

df = pd.DataFrame(output_records)

df.to_parquet(out_path, index=False)
print(f"written with {len(df):,.0f} records")
