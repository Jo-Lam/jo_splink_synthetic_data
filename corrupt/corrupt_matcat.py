def gen_uncorrupted_matcat(formatted_master_record, record_to_modify={}):

    record_to_modify["maternal_agecat"] = formatted_master_record["maternal_agecat"]

    return record_to_modify