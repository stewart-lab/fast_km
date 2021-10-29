import workers.loaded_index as li

def km_work(json):
    return [{"cancer" : len(li.the_index.query_index("cancer"))}]