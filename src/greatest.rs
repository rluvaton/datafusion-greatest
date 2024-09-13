use crate::helpers::{find_coerced_type, is_larger_or_equal, keep_larger};
use datafusion::error::Result;
use datafusion::logical_expr::Volatility;
use std::any::Any;
use datafusion::arrow::array::{make_comparator, new_null_array, Array, BooleanArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::kernels::cmp;
use datafusion::arrow::compute::{and, SortOptions};
use datafusion::arrow::compute::kernels::zip::zip;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion_common::{exec_err, DataFusionError};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use crate::traits::NullBufferExt;

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
            signature: Signature::user_defined(Volatility::Immutable),
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
        find_coerced_type(arg_types).cloned().ok_or_else(|| {
            DataFusionError::Internal(
                "Could not find a common type for the arguments".to_string(),
            )
        })
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

        let return_type = args[0].data_type();

        // TODO - different size arrays
        let return_array_size = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Array(array) => Some(array.len()),
                _ => None,
            })
            .next()

            // 1 in the case of scalar
            .unwrap_or(1);


        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, return_array_size);

        for arg in args {
            match arg {
                ColumnarValue::Array(ref array) => {
                    current_value = keep_larger(array.clone(), current_value)?;
                }
                ColumnarValue::Scalar(value) => {
                    // If null skip to avoid creating array full of nulls
                    if value.is_null() {
                        continue;
                    }


                    // TODO - this has bad performance
                    let last_value = value.to_array_of_size(return_array_size)?;
                    current_value = keep_larger(last_value, current_value)?;
                }
            }
        }

        Ok(ColumnarValue::Array(current_value))
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
            return exec_err!(
                "greatest was called with {} arguments. It requires at least 2.",
                input_types.len()
            );
        }

        // Make sure we can do the comparison,
        // similar to: https://github.com/apache/spark/blob/19aad9ee36edad0906b8223074351bfb76237c0a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L1287-L1295
        let coerced_type = find_coerced_type(input_types);

        // If we can't find a common type, we can't continue
        if coerced_type.is_none() {
            return exec_err!("greatest cannot resolve the input types {:?}", input_types);
        }

        let coerced_type = coerced_type.unwrap();
        Ok(vec![coerced_type.clone(); input_types.len()])
    }
}

/// TODO - test not represented 0.3 and 0.29999999999999999

#[cfg(test)]
mod tests {
    use crate::greatest::GreatestUdf;
    use datafusion::dataframe::DataFrame;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{col, lit, ScalarUDF};
    use std::sync::Arc;
    use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch};

    /// create local execution context with an in-memory table:
    ///
    /// ```text
    /// +-----+-----+
    /// | a   | b   |
    /// +-----+-----+
    /// |  2  |  10 |
    /// |  3  |  2  |
    /// |  4  |  5  |
    /// |  5  |  1  |
    /// |  6  | 102 |
    /// |  7  |  4  |
    /// | 150 |  6  |
    /// |  1  |  2  |
    /// +-----+-----+
    /// ```
    fn create_context() -> Result<(SessionContext, Vec<Vec<f64>>)> {
        let a_vec = vec![2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 150.0, 1.0];
        let b_vec = vec![10.0, 2.0, 5.0, 1.0, 102.0, 4.0, 6.0, 2.0];
        let c_vec = vec![3420.0, 10.0, 2.0, 43.0, 3.0, 56.0, 12.0, 33.0];
        let d_vec = vec![2.0, 2.0, 3.0, 5.0, 9.0, 8.0, 6.0, 244.0];
        // define data.
        let a: ArrayRef = Arc::new(Float64Array::from(a_vec.clone()));
        let b: ArrayRef = Arc::new(Float64Array::from(b_vec.clone()));
        let c: ArrayRef = Arc::new(Float64Array::from(c_vec.clone()));
        let d: ArrayRef = Arc::new(Float64Array::from(d_vec.clone()));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c), ("d", d)])?;

        // declare a new context. In Spark API, this corresponds to a new SparkSession
        let ctx = SessionContext::new();

        // declare a table in memory. In Spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch)?;


        Ok((ctx, vec![
            a_vec,
            b_vec,
            c_vec,
            d_vec,
        ]))
    }

    fn get_expected_greatest(cols: Vec<Vec<f64>>) -> Vec<f64> {

        // Convert vectors of columns to vectors of rows
        let mut rows = vec![];
        for i in 0..cols.len() {
            for j in 0..cols[i].len() {
                if rows.len() <= j {
                    rows.push(vec![]);
                }
                rows[j].push(cols[i][j]);
            }
        }

        rows.iter().map(|row| {
            [row.clone(), vec![f64::NAN]].concat()
                .into_iter()
                .reduce(f64::max)
                .unwrap()
            // *row.iter().max().unwrap()
        }).collect()
    }


    async fn setup() -> Result<(SessionContext, DataFrame, ScalarUDF, Vec<Vec<f64>>)> {
        // In this example we register `GreatestUdf` as a user defined function
        // and invoke it via the DataFrame API and SQL
        let (ctx, data) = create_context()?;

        // create the UDF
        let greatest = ScalarUDF::from(GreatestUdf::new());

        // register the UDF with the context so it can be invoked by name and from SQL
        ctx.register_udf(greatest.clone());

        // get a DataFrame from the context for scanning the "t" table
        let df = ctx.table("t").await?;

        Ok((ctx, df, greatest, data))
    }

    #[tokio::test]
    async fn sample_api() {
        let (ctx, df, greatest, all_data) = setup().await.unwrap();
        let original_df = df.clone();

        // Call pow(a, 10) using the DataFrame API
        // let df = df.select(vec![greatest.call(vec![col("a"), lit(10i32)])])?;
        let df = original_df.clone().select(vec![greatest.call(vec![col("a"), col("b"), lit(20f64)])]).unwrap();

        // note that the second argument is passed as an i32, not f64. DataFusion
        // automatically coerces the types to match the UDF's defined signature.

        // print the results
        df.clone().show().await.unwrap();

        let results = df.clone().collect().await.unwrap();
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        let columns = batch.columns();
        assert_eq!(columns.len(), 1);
        let column = &columns[0];
        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
        let actual_greatest = array.values().iter().map(|v| *v).collect::<Vec<_>>();
        assert_eq!(actual_greatest, get_expected_greatest(vec![all_data[0].clone(), all_data[1].clone()]));


        // Call pow(a, 10) using the DataFrame API
        // let df = df.select(vec![greatest.call(vec![col("a"), lit(10i32)])])?;
        let df = original_df.clone().select(vec![greatest.call(vec![
            col("a"),
            col("b"),
            col("c"),
            col("d"),
        ])]).unwrap();

        // note that the second argument is passed as an i32, not f64. DataFusion
        // automatically coerces the types to match the UDF's defined signature.

        // print the results
        df.show().await.unwrap();
    }

    #[tokio::test]
    async fn sample_sql() {
        let (ctx, df, greatest, all_data) = setup().await.unwrap();

        // You can also invoke both pow(2, 10)  and its alias my_pow(a, b) using SQL
        let sql_df = ctx.sql("SELECT greatest(2, 10), greatest(a, b) FROM t").await.unwrap();
        sql_df.show().await.unwrap();
    }
}
