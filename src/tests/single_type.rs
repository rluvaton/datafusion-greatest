#[cfg(test)]
mod tests {
    // TODO - run tests for each column when there is only one column

    use datafusion::arrow::array::{Int8Array, RecordBatch};
    use datafusion::arrow::datatypes::{Int64Type, Int8Type};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;


    // TODO - create helpers for testing

    use crate::greatest::GreatestUdf;
    use crate::tests::utils::{create_context, find_greatest, get_result_as_matrix};
    use datafusion::arrow::array::{ArrayRef, Float64Array};
    use datafusion::dataframe::DataFrame;
    use datafusion::error::Result;
    use datafusion_expr::{col, ScalarUDF};

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
    fn create_context_2() -> Result<(SessionContext, Vec<Vec<f64>>)> {


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

    async fn setup() -> Result<(SessionContext, DataFrame, ScalarUDF, Vec<Vec<f64>>)> {
        // In this example we register `GreatestUdf` as a user defined function
        // and invoke it via the DataFrame API and SQL
        let (ctx, data) = create_context_2()?;

        // create the UDF
        let greatest = ScalarUDF::from(GreatestUdf::new());

        // register the UDF with the context so it can be invoked by name and from SQL
        ctx.register_udf(greatest.clone());

        // get a DataFrame from the context for scanning the "t" table
        let df = ctx.table("t").await?;

        Ok((ctx, df, greatest, data))
    }

    #[tokio::test]
    async fn i8() {
        let (ctx, greatest) = create_context();

        // define data.
        let a_vec = vec![2, 3, 4, 5, 6, 7, -10, 1];
        let b_vec = vec![10, 2, 5, 1, 102, 4, 6, 2];
        let a: ArrayRef = Arc::new(Int8Array::from(a_vec.clone()));
        let b: ArrayRef = Arc::new(Int8Array::from(b_vec.clone()));
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        // declare a table in memory. In Spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();


        // get a DataFrame from the context for scanning the "t" table
        let df = ctx.table("t").await.unwrap();


        let original_df = df.clone();

        // Call pow(a, 10) using the DataFrame API
        let df = original_df.clone().select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn sample_sql() {
        let (ctx, df, greatest, all_data) = setup().await.unwrap();

        // You can also invoke both pow(2, 10)  and its alias my_pow(a, b) using SQL
        let sql_df = ctx.sql("SELECT greatest(2, 10), my_greatest(a, b) FROM t").await.unwrap();
        sql_df.show().await.unwrap();
    }
}
