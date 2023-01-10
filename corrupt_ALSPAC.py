import argparse
from pathlib import Path
import os
import logging
import pandas as pd
import numpy as np
import duckdb

# create path and transform .csv gold to parquet
df = pd.read_csv('ALSPAC_syn_gold.csv')
df.to_parquet('transformed_master_data.parquet')
pd.read_parquet('transformed_master_data.parquet', engine='pyarrow')


# This Block Introduces the corruption functions 

from corrupt.corruption_functions import (
    master_record_no_op,
    format_master_data,
    alspac_generate_uncorrupted_output_record,
)

from corrupt.corrupt_name import (
    alspac_G1_first_name_gen_uncorrupted_record,
    alspac_G1_surname_gen_uncorrupted_record,
    alspac_G0_surname_gen_uncorrupted_record,
    alspac_first_name_random, # 6% completely different
    alspac_G1_surname_random, # 95% completely different 
    alspac_G0_surname_random, # 95% completely different 
    alspac_first_name_alternatives, # 73% alternatives
    alspac_first_name_insertion,
    alspac_first_name_deletion,
    alspac_G1_last_name_insertion,
    alspac_G1_last_name_deletion,
    alspac_G0_last_name_insertion,
    alspac_G0_last_name_deletion,
    alspac_first_name_typo, # 14% typo
)

from corrupt.corrupt_id import (
    gen_uncorrupted_id
)

from corrupt.corrupt_date import (
    gen_uncorrupted_date_alspac,
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
date = "2023_01_09"
ALSPAC_corrupt_outpath = os.path.join(output,date) #where to deposit the corrupted data

from corrupt.record_corruptor import (
    ProbabilityAdjustmentFromLookup,
    RecordCorruptor,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(message)s",
)
logger.setLevel(logging.INFO)


con = duckdb.connect()

# change path to gold.

in_path = os.path.join("transformed_master_data.parquet")


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
        "col_name": "gender_syn",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_gender,
    },
    {
        "col_name": "imddecile",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_imd,
    },
    {
        "col_name": "g1_dob_arc1",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": gen_uncorrupted_date_alspac,
    },
    {
        "col_name": "G0_surname_syn",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": alspac_G0_surname_gen_uncorrupted_record,
    },
    {
        "col_name": "G1_surname_syn",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": alspac_G1_surname_gen_uncorrupted_record,
    },
        {
        "col_name": "G1_firstname_syn",
        "format_master_data": master_record_no_op,
        "gen_uncorrupted_record": alspac_G1_first_name_gen_uncorrupted_record,
    },
]


rc = RecordCorruptor()

########
# Name-based corruptions
########

rc.add_simple_corruption(
    name="random_first",
    corruption_function=alspac_first_name_random,
    args={},
    baseline_probability=0.1,
)
rc.add_simple_corruption(
    name="first_name_variants",
    corruption_function=alspac_first_name_alternatives,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="first_name_deletion",
    corruption_function=alspac_first_name_deletion,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="first_name_insertion",
    corruption_function=alspac_first_name_insertion,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="first_name_typo",
    corruption_function=alspac_first_name_typo,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="G0_last_name_deletion",
    corruption_function=alspac_G0_last_name_deletion,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="G0_last_name_insertion",
    corruption_function=alspac_G0_last_name_insertion,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="G0_last_name_random",
    corruption_function=alspac_G0_surname_random,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="G1_last_name_deletion",
    corruption_function=alspac_G1_last_name_deletion,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="G1_last_name_insertion",
    corruption_function=alspac_G1_last_name_insertion,
    args={},
    baseline_probability=0.1,
)

rc.add_simple_corruption(
    name="G1_last_name_random",
    corruption_function=alspac_G1_surname_random,
    args={},
    baseline_probability=0.1,
)


adjustment_lookup = {
    "ethgroup": {
        "White": [(alspac_first_name_random, 1),
         (alspac_first_name_alternatives, 1),
         (alspac_first_name_deletion, 1), 
        (alspac_first_name_insertion, 1), 
        (alspac_first_name_typo, 1), 
        (alspac_G0_last_name_deletion, 1),
        (alspac_G0_last_name_insertion, 1),
        (alspac_G0_surname_random, 1) ,
        (alspac_G1_last_name_deletion, 1),
        (alspac_G1_last_name_insertion, 1),
        (alspac_G1_surname_random, 1)],
        "Black":[(alspac_first_name_random, 1.19),
        (alspac_first_name_deletion,1.19) , 
        (alspac_first_name_alternatives,1.19) , 
        (alspac_first_name_insertion,1.19) , 
        (alspac_first_name_typo,1.19) , 
        (alspac_G0_last_name_deletion,1.05) ,
        (alspac_G0_last_name_insertion,1.05) ,
        (alspac_G0_surname_random,1.05) ,
        (alspac_G1_last_name_deletion,1.43) ,
        (alspac_G1_last_name_insertion,1.43) ,
        (alspac_G1_surname_random, 1.43)],
        "Other":[(alspac_first_name_random, 1.13),
        (alspac_first_name_alternatives, 1.13),
        (alspac_first_name_deletion, 1.13), 
        (alspac_first_name_insertion, 1.13), 
        (alspac_first_name_typo, 1.13), 
        (alspac_G0_last_name_deletion, 1.08),
        (alspac_G0_last_name_insertion, 1.08),
        (alspac_G0_surname_random,  1.08),
        (alspac_G1_last_name_deletion, 2.05),
        (alspac_G1_last_name_insertion, 2.05),
        (alspac_G1_surname_random, 2.05)],
        "Asian":[(alspac_first_name_random, 0.99),
        (alspac_first_name_alternatives, 0.99),
        (alspac_first_name_deletion, 0.99), 
        (alspac_first_name_insertion, 0.99), 
        (alspac_first_name_typo,0.99), 
        (alspac_G0_last_name_deletion, 0.46),
        (alspac_G0_last_name_insertion, 0.46),
        (alspac_G0_surname_random, 0.46),
        (alspac_G1_last_name_deletion, 0.34),
        (alspac_G1_last_name_insertion,0.34),
        (alspac_G1_surname_random, 0.34)],
    },
    "maternal_agecat":{
        "<20":[(alspac_first_name_random, 0.57),
        (alspac_first_name_alternatives, 0.57),
        (alspac_first_name_deletion, 0.57), 
        (alspac_first_name_insertion, 0.57), 
        (alspac_first_name_typo, 0.57), 
        (alspac_G0_last_name_deletion, 2.89),
        (alspac_G0_last_name_insertion, 2.89),
        (alspac_G0_surname_random, 2.89),
        (alspac_G1_last_name_deletion, 2.60),
        (alspac_G1_last_name_insertion, 2.60),
        (alspac_G1_surname_random,2.60)],
        "20-29":[(alspac_first_name_random, 0.85),
        (alspac_first_name_alternatives, 0.85),
        (alspac_first_name_deletion, 0.85), 
        (alspac_first_name_insertion, 0.85), 
        (alspac_first_name_typo, 0.85), 
        (alspac_G0_last_name_deletion, 1.61),
        (alspac_G0_last_name_insertion,1.61) ,
        (alspac_G0_surname_random,  1.61),
        (alspac_G1_last_name_deletion, 1.43),
        (alspac_G1_last_name_insertion, 1.43),
        (alspac_G1_surname_random, 1.43)],
        "30-39":[(alspac_first_name_random, 1),
        (alspac_first_name_deletion, 1), 
        (alspac_first_name_alternatives, 1), 
        (alspac_first_name_insertion, 1), 
        (alspac_first_name_typo, 1), 
        (alspac_G0_last_name_deletion, 1),
        (alspac_G0_last_name_insertion, 1),
        (alspac_G0_surname_random, 1),
        (alspac_G1_last_name_deletion, 1),
        (alspac_G1_last_name_insertion, 1),
        (alspac_G1_surname_random, 1)],
        "40+":[(alspac_first_name_random, 1.09),
        (alspac_first_name_deletion, 1.09), 
        (alspac_first_name_alternatives, 1.09), 
        (alspac_first_name_insertion, 1.09), 
        (alspac_first_name_typo, 1.09), 
        (alspac_G0_last_name_deletion, 1.14),
        (alspac_G0_last_name_insertion, 1.14),
        (alspac_G0_surname_random,  1.14),
        (alspac_G1_last_name_deletion, 1.44),
        (alspac_G1_last_name_insertion, 1.44),
        (alspac_G1_surname_random, 1.44)],
        "NA":[(alspac_first_name_random, 1.24),
        (alspac_first_name_deletion, 1.24), 
        (alspac_first_name_alternatives, 1.24), 
        (alspac_first_name_insertion, 1.24), 
        (alspac_first_name_typo, 1.24), 
        (alspac_G0_last_name_deletion, 1.50),
        (alspac_G0_last_name_insertion, 1.50),
        (alspac_G0_surname_random,  1.50),
        (alspac_G1_last_name_deletion, 2.13),
        (alspac_G1_last_name_insertion, 2.13),
        (alspac_G1_surname_random,2.13)],
    }
}

adjustment = ProbabilityAdjustmentFromLookup(adjustment_lookup)
rc.add_probability_adjustment(adjustment)

max_corrupted_records = 20
zipf_dist = get_zipf_dist(max_corrupted_records)


pd.options.display.max_columns = 1000
pd.options.display.max_colwidth = 1000

Path(ALSPAC_corrupt_outpath).mkdir(parents=True, exist_ok=True)


""" for 
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="data_linking job runner")

    parser.add_argument("--start_year", type=int)
    parser.add_argument("--num_years", type=int)
    args = parser.parse_args()
    start_year = args.start_year
    num_years = args.num_years
"""


# for year in range(1991, 1993):

out_path = os.path.join(ALSPAC_corrupt_outpath, f"corrupted.parquet")

# if os.path.exists(out_path):
#    continue

sql = f"""
select *
from '{in_path}'

"""
# where
#    year(try_cast(g1_dob[1] as date)) = {year}

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

    for i in range(total_num_corrupted_records):
        record_to_modify = uncorrupted_output_record.copy()
        record_to_modify["corruptions_applied"] = []
        record_to_modify["id"] = (
            uncorrupted_output_record["cluster"] + i+1
        )
        record_to_modify["uncorrupted_record"] = False
        rc.apply_probability_adjustments(uncorrupted_output_record)
        corrupted_record = rc.apply_corruptions_to_record(
            formatted_master_record,
            record_to_modify,
        )
        output_records.append(corrupted_record)

df = pd.DataFrame(output_records)

df.to_parquet(out_path, index=False)
print(f"written {len(df):,.0f} records")
# print(f"written {year} with {len(df):,.0f} records")