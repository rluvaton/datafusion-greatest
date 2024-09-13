use datafusion::arrow::array::{ArrowPrimitiveType, PrimitiveArray};
use datafusion::arrow::compute::concat_batches;
use datafusion::error::Result;
use datafusion::prelude::DataFrame;

/// This will return the result of a DataFrame as a matrix.
/// Each row in the matrix corresponds to a column in the DataFrame.
/// Each column in the matrix corresponds to a row in the DataFrame.
///
/// If you want to have the rows in the matrix to correspond to the rows in the DataFrame,
/// you can use the `transpose` method on the resulting matrix.
pub(crate) async fn get_result_as_matrix<PrimitiveType: ArrowPrimitiveType>(df: DataFrame) -> Result<Vec<Vec<PrimitiveType::Native>>> {
    let results = df.clone().collect().await?;

    let all_batches_as_one = concat_batches(df.schema().as_ref(), results.iter())?;
    let columns = all_batches_as_one.columns();

    Ok(
        columns
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<PrimitiveArray<PrimitiveType>>()
                    .expect("Unable to downcast to expected PrimitiveArray")
                    .values()
                    .iter()
                    .map(|v| *v)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int8Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int8Type, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_get_result_as_matrix_i32() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));


        let a_vec: Vec<i32> = vec![1, 10, 10, 100];
        let b_vec: Vec<i32> = vec![4, 20, 30, 500];
        let c_vec: Vec<i32> = vec![5, 70, 60, 700];


        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a_vec.clone())),
                Arc::new(Int32Array::from(b_vec.clone())),
                Arc::new(Int32Array::from(c_vec.clone())),
            ],
        ).unwrap();

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let df = df.select_columns(&["a", "b", "c"]).unwrap();
        let results = get_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            a_vec,
            b_vec,
            c_vec
        ])
    }

    #[tokio::test]
    async fn test_get_result_as_matrix_i8() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int8, false),
            Field::new("b", DataType::Int8, false),
            Field::new("c", DataType::Int8, false),
        ]));


        let a_vec: Vec<i8> = vec![1, 10, 10, 100];
        let b_vec: Vec<i8> = vec![4, 20, 30, 40];
        let c_vec: Vec<i8> = vec![5, 70, 60, 80];


        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int8Array::from(a_vec.clone())),
                Arc::new(Int8Array::from(b_vec.clone())),
                Arc::new(Int8Array::from(c_vec.clone())),
            ],
        ).unwrap();

        // declare a new context. In spark API, this corresponds to a new spark SQLsession
        let ctx = SessionContext::new();

        // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let df = df.select_columns(&["a", "b", "c"]).unwrap();
        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            a_vec,
            b_vec,
            c_vec
        ])
    }
}

