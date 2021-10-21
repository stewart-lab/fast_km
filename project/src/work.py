import project.src.loaded_index as lindx

def km_work(json):
    return [{"cancer" : len(lindx.the_index.query_index("cancer"))}]