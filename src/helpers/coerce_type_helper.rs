use datafusion::arrow::datatypes::DataType;
use datafusion_expr::type_coercion::functions::can_coerce_from;
use datafusion::error::Result;
use datafusion_common::plan_err;

pub(crate) fn find_coerced_type(data_types: &[DataType]) -> Result<&DataType> {
    let mut non_null_types = data_types.iter().filter(|t| {
        !t.is_null()
    }).into_iter();

    let first_type = non_null_types.next();

    // If every type is null, return null
    if first_type.is_none() {
        return Ok(&DataType::Null);
    }

    let first_type = first_type.unwrap();

    let mut current_type = first_type;

    for t in non_null_types {
        if t == current_type {
            continue;
        }

        // TODO(rluvaton): It might be beneficial to cache the result of can_coerce_from
        let can_coerce_current_to_type = can_coerce_from(t, current_type);
        let can_coerce_type_to_current = can_coerce_from(current_type, t);

        // If we can coerce from the current type to the new type and vice versa, we can continue
        if can_coerce_current_to_type && can_coerce_type_to_current {
            continue;
        }

        // If we can't coerce from the current type to the new type and vice versa, we can't continue
        // as we can't find a common type
        if !can_coerce_current_to_type && !can_coerce_type_to_current {
            return plan_err!("Cannot find a common type for arguments, incompatible {} and {}", current_type, t);
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

    Ok(current_type)
}
