import numpy as np


def generate_error_vectors(config, num_error_vectors_to_generate):
    """
    An error vector is a succinct description of how corruptions will be introduced
    into an original, master record

    For each original record, multiple error vectors can be generated,
    resulting in n duplicate records being generated.


    The error vector is in the format

    {
        output_col_name: corruption_function_index
    }

    For example:

    {
        full_name: 2,
        dob: 1,
        occuption: 0,
        etc.
    }

    The codings for the corruption function index are as follows:

    -1: Output a null
     0: Do nothing (i.e. leave original data )
     1: Use the first error function speficied in the config
     ..
     n: Use the nth error function specified in the config

    """

    # The following is an extremely simple implementation with no correlations!

    list_error_vectors = []

    for this_vector in range(num_error_vectors_to_generate):
        error_vector = {}
        for entry in config:
            col_name = entry["col_name"]
            corruption_functions = entry["corruption_functions"]
            num_corruption_functions = len(corruption_functions)

            null_probability = 0.1
            do_nothing_probability = 0.5
            probability_corrupt = 1 - null_probability - do_nothing_probability

            reweighted_corruption_probabilities = [
                f["p"] * probability_corrupt for f in corruption_functions
            ]

            # i.e. if there are two corruption functions, this will be [-1, 0, 1, 2]
            error_vector_values = [-1] + list(range(num_corruption_functions + 1))

            error_vector_weights = [
                null_probability,
                do_nothing_probability,
            ] + reweighted_corruption_probabilities

            chosen_error_vector_value = np.random.choice(
                error_vector_values, p=error_vector_weights
            )
            error_vector[col_name] = chosen_error_vector_value
        list_error_vectors.append(error_vector)
    return list_error_vectors


def apply_error_vector(error_vector, formatted_master_record, config):
    """
    Use an error vector to corrupt a record
    """
    output_record = {}
    for output_col in config:
        output_col_name = output_col["col_name"]
        null_fn = output_col["null_function"]
        no_change_fn = output_col["gen_uncorrupted_record"]
        error_vector_value = error_vector[output_col_name]

        corruption_functions = [c["fn"] for c in output_col["corruption_functions"]]
        if error_vector_value == -1:
            fn = null_fn
        elif error_vector_value == 0:
            fn = no_change_fn
        else:
            fn = corruption_functions[error_vector_value - 1]

        output_record = fn(formatted_master_record, record_to_modify=output_record)

    return output_record

# Step 0: Group Data into Categories
# Load in Master data, create 2 new column, recode. [probably best to represent this in a separate page]

# Error introduction (Missing is counted as a type of error)
# Error DAG: http://dagitty.net/mFAN9cX

# Affiliate Variables: Century of Death (1600s, 1700s, 1800s, 1900s), Ethnicity (White, Black, Multiple Ethnic, Asian, Other)
# Linkage Variables: First Name, Last Name, Date of Birth, Gender


# Retain dependencies between: 
# Value of Affiliate with Error in Linkage
# Error in Linkage with Error in Linkage (e.g., People with Errors in First Name are also likely to have Errors in Last Name)


### If YoD = 1600s, Error in linkage variable = more likely, Error induction mechanism = Mainly Missing... same logic flows for all linkage variables
### If Ethnicity =  (White, Black, Multiple, Asian, Other, Missing), Error in 

#First Name
if col_name = [FirstName]:
    if ethnic_cat = [White] and CoD = [1600s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Black] and CoD = [1600s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Multiple] and CoD = [1600s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Asian] and CoD = [1600s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Other] and CoD = [1600s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [White] and CoD = [1700s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Black] and CoD = [1700s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Multiple] and CoD = [1700s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Asian] and CoD = [1700s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
   elif ethnic_cat = [Other] and CoD = [1700s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
       if ethnic_cat = [White] and CoD = [1800s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Black] and CoD = [1800s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Multiple] and CoD = [1800s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Asian] and CoD = [1800s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Other] and CoD = [1800s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [White] and CoD = [1900s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Black] and CoD = [1900s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Multiple] and CoD = [1900s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
    elif ethnic_cat = [Asian] and CoD = [1900s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability
   elif ethnic_cat = [Other] and CoD = [1900s]:
        null_probability = 0.1
        do_nothing_probability = 0.5
        probability_corrupt = 1 - null_probability - do_nothing_probability

#Last Name, Ethnic and CoD Values + First Name Errors (3 conditions, combination of 5 X 4 X 2)
if col_name = last_name:
    

    
    
 
