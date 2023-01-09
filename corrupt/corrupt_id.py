def gen_uncorrupted_id(formatted_master_record, record_to_modify={}):

    record_to_modify["random_id"] = formatted_master_record["random_id"]

    return record_to_modify