use datafusion::arrow::datatypes::DataType;
use datafusion_expr::type_coercion::functions::can_coerce_from;
use datafusion::error::Result;
use datafusion_common::plan_err;

pub(crate) fn find_coerced_type(data_types: &[DataType]) -> Result<&DataType> {
    let non_null_types = data_types.iter().filter(|t| {
        !t.is_null()
    }).collect::<Vec<_>>();

    if non_null_types.is_empty() {
        return Ok(&DataType::Null);
    }

    let non_null_types_clone = non_null_types.clone();

    for data_type in non_null_types_clone {
        let can_coerce_to_all = non_null_types.iter().all(|t| can_coerce_from(data_type, t));

        if can_coerce_to_all {
            return Ok(data_type);
        }
    }

    // For better error messages, we can find the ones that are incompatible with the rest
    plan_err!("Cannot find a common type for arguments, data types: {:?}", data_types)
}
