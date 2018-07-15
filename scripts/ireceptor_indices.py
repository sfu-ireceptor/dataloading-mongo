"""The list of standard indices iReceptor has on the sequence level for collections in the Mongo repositories"""

__author__ = ("Colin Qiao <colinq@sfu.ca>")


I = [{
    "ir_project_sample_id": 1
}, {
    "ir_project_sample_id": 1,
    "v_call": 1
}, {
    "ir_project_sample_id": 1,
    "j_call": 1
}, {
    "ir_project_sample_id": 1,
    "d_call": 1
}, {
    "ir_project_sample_id": 1,
    "junction_aa_length": 1
}, {
    "annotation_tool": 1
}, {
    "ir_project_sample_id": 1,
    "vgene_family": 1
}, {
    "ir_project_sample_id": 1,
    "vgene_gene": 1
}, {
    "ir_project_sample_id": 1,
    "jgene_gene": 1
}, {
    "ir_project_sample_id": 1,
    "jgene_family": 1
}, {
    "ir_project_sample_id": 1,
    "dgene_gene": 1
}, {
    "ir_project_sample_id": 1,
    "dgene_family": 1
}, {
    "ir_project_sample_id": 1,
    "functional": 1
}, {
    "ir_project_sample_id": 1,
    "annotation_tool": 1
}, {
    "substring": 1,
    "ir_project_sample_id": 1
}]

def indices():
    """Return a list of list of tuples containing the name of index and its value"""

    indices = []
    for i in I:
        index = []
        for k, v in i.items():
            index.append((k, v))
        indices.append(index)
    return indices

indices = indices()