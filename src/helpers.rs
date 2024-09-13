use datafusion::arrow::datatypes::DataType;
use datafusion_expr::type_coercion::functions::can_coerce_from;

pub fn find_coerced_type(data_types: &[DataType]) -> Option<&DataType> {
    let first_type = data_types.first().expect("Must have at least one type");

    // TODO - avoid comparing the first type twice
    // can_coerce_from

    let mut current_type = first_type;

    for t in data_types.iter() {
        if t == current_type {
            continue;
        }

        // TODO - cache?
        let can_coerce_current_to_type = can_coerce_from(current_type, t);
        let can_coerce_type_to_current = can_coerce_from(t, current_type);

        // If we can coerce from the current type to the new type and vice versa, we can continue
        if can_coerce_current_to_type && can_coerce_type_to_current {
            continue;
        }

        // If we can't coerce from the current type to the new type and vice versa, we can't continue
        // as we can't find a common type
        if !can_coerce_current_to_type && !can_coerce_type_to_current {
            return None;
        }

        // i64 -> i32 (no) | i32 -> i64 (yes)
        // If can coerce from the current type to the new type, but vice versa is not possible
        // than we need to use the new type as the current type
        //
        // Example:
        // if current type is i32 and new type is i64
        // we can coerce from i32 to i64 without loss of data
        // but we can't coerce from i64 to i32 without loss of data
        if can_coerce_current_to_type {
            current_type = t;
        }
    }

    Some(current_type)
}
