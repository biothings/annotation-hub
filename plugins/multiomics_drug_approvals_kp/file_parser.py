import csv
import os
import json

attribute_source = "infores:biothings-multiomics-drugapprovals"
attribute_data_source = "infores:isb-wellness"
faers = "infores:faers"
dailymed = "infores:dailymed"
kgInfoUrl = "https://db.systemsbiology.net/gestalt/cgi-pub/KGinfo.pl?id="


def load_data(data_folder):
    edges_file_path = os.path.join(data_folder, "drug_approvals_kg_edges_v0.1.tsv")
    nodes_file_path = os.path.join(data_folder, "drug_approvals_kg_nodes_v0.1.tsv")
    nodes_f = open(nodes_file_path)
    edges_f = open(edges_file_path)
    nodes_data = csv.reader(nodes_f, delimiter="\t")
    edges_data = csv.reader(edges_f, delimiter="\t")

    headers = next(nodes_data)
    id_name_mapping = {}
    id_type_mapping = {}
    for line in nodes_data:
        id_name_mapping[line[0]] = line[1]
        id_type_mapping[line[0]] = line[2]

    headers = next(edges_data)
    for line in edges_data:
        if line[0] and line[1] and line[0].split(":")[0] and line[2].split(":")[0]:

            prefix = line[0].split(":")[0].replace(".", "_")
            subject = {
                "id": line[0],
                prefix: line[0],
                "name": id_name_mapping[line[0]],
                "type": id_type_mapping[line[0]],
            }

            prefix = line[2].split(":")[0].replace(".", "_")
            object_ = {
                "id": line[2],
                prefix.lower(): line[2],
                "name": id_name_mapping[line[2]],
                "type": id_type_mapping[line[2]],
            }

            # properties for predicate/association
            edge_attributes = []

            # knowledge level
            edge_attributes.append(
                {
                    "attribute_type_id": "biolink:knowledge_level",
                    "value": line[7],
                }
            )

            # agent type
            edge_attributes.append(
                {
                    "attribute_type_id": "biolink:agent_type",
                    "value": line[8],
                }
            )

            # approval status
            edge_attributes.append(
                {
                    "attribute_type_id": "clinical_approval_status",
                    "value": "approved_for_condition" if line[1] == "treats" else "not_approved_for_condition",
                }
            )

            # approval NDAs
            # edge_attributes.append(
            #    {
            #        "attribute_type_id": "approvals",
            #        "value": line[9],
            #     }
            # )

            # supporting SPLs
            # edge_attributes.append(
            #    {
            #        "attribute_type_id": "supporting SPLs",
            #        "value": line[11],
            #     }
            # )

            # sources
            edge_sources = [
                {
                    "resource_id": attribute_source,
                    "resource_role": "primary_knowledge_source",
                    "source_record_urls": [kgInfoUrl + line[12]],
                },
                {"resource_id": dailymed, "resource_role": "supporting_data_source"},
                {"resource_id": faers, "resource_role": "supporting_data_source"},
            ]

            association = {"label": line[1], "attributes": edge_attributes, "sources": edge_sources}

            # Yield subject, predicate, and object properties
            data = {"_id": line[12], "subject": subject, "association": association, "object": object_}

            yield data

        else:
            print(f"Cannot find prefix for {line} !")


def main():
    testing = False  # True
    done = 0
    gen = load_data("test")
    while not testing or done < 10:
        try:
            entry = next(gen)
        except:
            break
        else:
            print(json.dumps(entry, sort_keys=True, indent=2))
            done = done + 1
    # print(done)


if __name__ == "__main__":
    main()
