from mecon.tags import tagging


def add_rule_for_id(tag: tagging.Tag, ids_to_add: str | list[str]) -> tagging.Tag:
    ids_to_add = ids_to_add if isinstance(ids_to_add, list) else [ids_to_add]
    potential_id_condition = tag.rule.rules[0].rules[0]
    if potential_id_condition.field == 'id' and potential_id_condition.transformation_operation.name == 'none':
        existing_ids = set(potential_id_condition.value.split(','))
        non_existing_ids_to_add = set(ids_to_add).difference(existing_ids)

        if len(non_existing_ids_to_add) == 0:
            return tag
        merged_ids_str = ','.join(sorted(non_existing_ids_to_add))
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in',
                                                            f"{merged_ids_str},{potential_id_condition.value}")
        tag.rule.rules[0].rules[0] = id_condition
    else:
        merged_ids_str = ','.join(sorted(ids_to_add))
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in', merged_ids_str)
        tag = tagging.Tag(tag.name, tag.rule.append(tagging.Conjunction([id_condition])))
    return tag
