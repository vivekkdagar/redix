def parsed_resp_array(data: bytes) -> tuple[list[str], int]:
    if not data or not data.startswith(b"*"):
        return [], 0

    try:
        crlf_index = data.find(b"\r\n")
        if crlf_index == -1:
            return [], 0

        count_bytes = data[1:crlf_index]
        if not count_bytes:
            print("Parser Error: No element count found.")
            return [], 0

        num_elements_str = count_bytes.decode()
        num_elements = int(num_elements_str)

    except ValueError:
        print(f"Parser Error: Invalid element count value: {data[1:crlf_index]}")
        return [], 0

    parsed_elements = []
    index = crlf_index + 2

    print(f"Parser: Expecting {num_elements} elements.")

    for i in range(num_elements):
        if index >= len(data) or data[index: index + 1] != b"$":
            print(f"Parser Error: Element {i} not starting with $ at index {index}.")
            return [], 0

        index += 1

        crlf_index = data.find(b"\r\n", index)
        if crlf_index == -1:
            print(f"Parser Error: Element {i} missing length CRLF.")
            return [], 0

        try:
            length_bytes = data[index:crlf_index]
            str_length = int(length_bytes.decode())
            print(f"Parser: Element {i} length is {str_length}.")
        except ValueError:
            print(f"Parser Error: Element {i} invalid length value: {length_bytes}")
            return [], 0

        index = crlf_index + 2

        value_end_index = index + str_length
        if value_end_index + 2 > len(data):
            print(f"Parser Error: Element {i} incomplete data or missing trailing CRLF.")
            return [], 0

        value = data[index:value_end_index].decode()
        parsed_elements.append(value)
        print(f"Parser: Element {i} value: '{value}'")

        index = value_end_index + 2

    return parsed_elements, index