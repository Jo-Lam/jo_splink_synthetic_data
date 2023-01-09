def gen_uncorrupted_imd(formatted_master_record, record_to_modify={}):

    record_to_modify["imddecile"] = formatted_master_record["imddecile"]

    return record_to_modify