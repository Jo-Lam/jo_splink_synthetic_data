def gen_uncorrupted_ethgroup(formatted_master_record, record_to_modify={}):

    record_to_modify["ethgroup"] = formatted_master_record["ethgroup"]

    return record_to_modify