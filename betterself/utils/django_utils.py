def create_django_choice_tuple_from_list(list_a):
    if list_a is None:
        return ()

    tuples_list = []
    for item in list_a:
        tuple_item_title = item.title() if isinstance(item, str) else item
        tuple_item = (item, tuple_item_title)
        tuples_list.append(tuple_item)

    return tuple(tuples_list)
