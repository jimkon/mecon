from mecon.tag_tools import tagging


def add_rule_for_id(tag: tagging.Tag, _id_str: str) -> tagging.Tag:
    potential_id_condition = tag.rule.rules[0].rules[0]
    if potential_id_condition.field == 'id' and potential_id_condition.transformation_operation.name == 'none':
        if _id_str in potential_id_condition.value:
            return tag
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in',
                                                            f"{_id_str},{potential_id_condition.value}")
        tag.rule.rules[0].rules[0] = id_condition
    else:
        id_condition = tagging.Condition.from_string_values('id', 'none', 'in', _id_str)
        tag = tagging.Tag(tag.name, tag.rule.append(tagging.Conjunction([id_condition])))
    return tag
