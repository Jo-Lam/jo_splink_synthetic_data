def sex_or_gender_gen_uncorrupted_record(formatted_master_record, record_to_modify={}):
    record_to_modify["sex_or_gender"] = str(formatted_master_record["sex_or_gender"])
    return record_to_modify


def sex_or_gender_corrupt(formatted_master_record, record_to_modify={}):
    """Replace Male = Female, Female = Male"""
    
    options = formatted_master_record["sex_or_gender"]
    if options == "male":
        record_to_modify["sex_or_gender"] = "female"
    elif options == "female":
        record_to_modify["sex_or_gender"] = "male"
    return record_to_modify
