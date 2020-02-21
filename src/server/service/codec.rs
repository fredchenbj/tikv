use kvproto::kvrpcpb::Element;

pub fn decode_data(
    row_data: Vec<u8>,
    fields: &Vec<Vec<u8>>,
) -> std::result::Result<Vec<Element>, &'static str> {
    let size = row_data.len();
    if size == 0 {
        return Err("empty");
    }

    let mut new_elements = Vec::new();
    let data = row_data.as_slice();

    let mut begin = 0;
    begin += 4;

    while begin < size {
        //decode key size
        //let first = begin;
        let end = begin + 4;
        let vv = &data[begin..end];
        let ptr: *const u8 = vv.as_ptr();
        let ptr: *const u32 = ptr as *const u32;
        let key_size = unsafe { *ptr };

        //decode key
        begin = end;
        let end = begin + key_size as usize;
        let key = &data[begin..end];

        //decode value size
        begin = end;
        let end = begin + 4;
        let vv = &data[begin..end];
        let ptr: *const u8 = vv.as_ptr();
        let ptr: *const u32 = ptr as *const u32;
        let value_size = unsafe { *ptr };

        //decode value
        begin = end;
        let end = begin + value_size as usize;
        let value = &data[begin..end];

        let mut is_select = false;
        if fields.len() > 0 {
            for v in fields {
                if v.eq(&key.to_vec()) {
                    is_select = true;
                    break;
                }
            }
        } else {
            is_select = true;
        }

        if is_select {
            let mut ele = Element::new();
            ele.set_index(key.to_vec());
            ele.set_value(value.to_vec());
            new_elements.push(ele);
        }

        begin = end;
    }
    Ok(new_elements)
}
