use crate::helpers::{find_coerced_type, keep_larger, keep_larger_scalar};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use datafusion_common::plan_err;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use std::any::Any;

/// This example shows how to use the full ScalarUDFImpl API to implement a user
/// defined function. As in the `simple_udf.rs` example, this struct implements
/// a function that takes two arguments and returns the first argument raised to
/// the power of the second argument `a^b`.
///
/// To do so, we must implement the `ScalarUDFImpl` trait.
#[derive(Debug, Clone)]
pub struct GreatestUdf {
    signature: Signature,
    aliases: Vec<String>,
}

impl GreatestUdf {
    /// Create a new instance of the `GreatestUdf` struct
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::user_defined(
                // Deterministic
                Volatility::Immutable
            ),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for GreatestUdf {
    /// We implement as_any so that we can downcast the ScalarUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "greatest"
    }

    /// Return the "signature" of this function -- namely what types of arguments it will take
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// What is the type of value that will be returned by this function? In
    /// this case it will always be a constant value, but it could also be a
    /// function of the input types.
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        find_coerced_type(arg_types).cloned()
    }

    /// This is the function that actually calculates the results.
    ///
    /// This is the same way that functions built into DataFusion are invoked,
    /// which permits important special cases when one or both of the arguments
    /// are single values (constants). For example `greatest(a, 2)`
    ///
    /// However, it also means the implementation is more complex than when
    /// using `create_udf`.
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // DataFusion has arranged for the correct inputs to be passed to this
        // function, but we check again to make sure
        assert!(args.len() >= 2);

        // Split to scalars and arrays
        let (scalars, arrays): (Vec<_>, Vec<_>) = args.iter().partition(|x| match x {
            ColumnarValue::Scalar(_) => true,
            ColumnarValue::Array(_) => false,
        });

        let mut arrays_iter = arrays
            .iter()
            .map(|x| match x {
                ColumnarValue::Array(a) => a,
                _ => unreachable!(),
            });

        let first_array = arrays_iter.next();

        let mut largest: ArrayRef;

        // Merge all scalars into one to avoid recomputing
        if !scalars.is_empty() {
            let mut scalars_iter = scalars
                .iter()
                .map(|x| match x {
                    ColumnarValue::Scalar(s) => s,
                    _ => unreachable!(),
                });

            // We have at least one scalar
            let mut largest_scalar = scalars_iter.next().unwrap();

            for scalar in scalars_iter {
                largest_scalar = keep_larger_scalar(largest_scalar, scalar)?;
            }

            // If we only have scalars, return the largest one
            if arrays.is_empty() {
                return Ok(ColumnarValue::Scalar(largest_scalar.clone()));
            }

            // We have at least one array
            let first_array = first_array.unwrap();

            // Start with the largest value
            largest = keep_larger(
                first_array.clone(),
                largest_scalar.to_array_of_size(first_array.len())?
            )?;
        } else {
            // If we only have arrays, start with the first array
            // (We must have at least one array)
            largest = first_array.unwrap().clone();
        }

        for array in arrays_iter {
            largest = keep_larger(array.clone(), largest)?;
        }

        Ok(ColumnarValue::Array(largest))
    }

    /// We will also add an alias of "my_greatest"
    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // the greatest preserves the order of the input as it doesn't matter
        Ok(input[0].sort_properties)
    }

    /// What types can this function coerce its arguments to?
    fn coerce_types(&self, input_types: &[DataType]) -> Result<Vec<DataType>> {
        // make sure that the input types has at least 2 elements
        // Spark source: https://github.com/apache/spark/blob/8023504e69fdd037dea002e961b960fd9fa662ba/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L1283-L1286
        if input_types.len() < 2 {
            return plan_err!(
                "greatest was called with {} arguments. It requires at least 2.",
                input_types.len()
            );
        }

        // Make sure we can do the comparison,
        // similar to: https://github.com/apache/spark/blob/19aad9ee36edad0906b8223074351bfb76237c0a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L1287-L1295
        let coerced_type = find_coerced_type(input_types)?;

        Ok(vec![coerced_type.clone(); input_types.len()])
    }
}

