def gen_uncorrupted_gender(formatted_master_record, record_to_modify={}):
    record_to_modify["gender"] = str(formatted_master_record["gender"])
    return record_to_modify


def gender_corrupt(formatted_master_record, record_to_modify={}):
    """Replace Male = Female, Female = Male"""
    
    options = formatted_master_record["gender"]
    if options == "male":
        record_to_modify["gender"] = "female"
    elif options == "female":
        record_to_modify["gender"] = "male"
    return record_to_modify