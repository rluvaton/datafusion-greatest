#[cfg(test)]
mod tests {
    // TODO - run tests for each column when there is only one column

    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{Int16Type, Int8Type};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;


    // TODO - create helpers for testing

    use crate::greatest::GreatestUdf;
    use crate::tests::utils::{create_context, create_primitive_array, find_greatest, generate_optional_values, get_result_as_matrix};
    use datafusion::arrow::array::{ArrayRef, Float64Array};
    use datafusion::dataframe::DataFrame;
    use datafusion::error::Result;
    use datafusion_expr::{col, ScalarUDF};
    use rand::Rng;

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
    async fn i8_without_nulls() {
        let (ctx, greatest) = create_context();
        let mut rng = rand::thread_rng();

        // define data.
        let a_vec = Vec::from_iter((0..8).map(|_| Some(rng.gen::<i8>())));
        let b_vec = Vec::from_iter((0..8).map(|_| Some(rng.gen::<i8>())));
        let a: ArrayRef = create_primitive_array::<Int8Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int8Type>(b_vec.clone());

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
    async fn i8_with_nulls() {
        let (ctx, greatest) = create_context();

        // define data.
        let a_vec = vec![Some(1), None, Some(-10), Some(4), None, Some(120), Some(7), Some(30)];
        let b_vec = vec![Some(5), None, None, Some(-2), Some(10), Some(1), Some(23), None];
        let a: ArrayRef = create_primitive_array::<Int8Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int8Type>(b_vec.clone());
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
    async fn i16_without_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<i16>(100, Some(0.0));
        let b_vec = generate_optional_values::<i16>(100, Some(0.0));
        let a: ArrayRef = create_primitive_array::<Int16Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int16Type>(b_vec.clone());
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        // declare a table in memory. In Spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();

        // get a DataFrame from the context for scanning the "t" table
        let df = ctx.table("t").await.unwrap();


        let original_df = df.clone();

        // Call pow(a, 10) using the DataFrame API
        let df = original_df.clone().select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int16Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn sample_sql() {
        let (ctx, df, greatest, all_data) = setup().await.unwrap();

        // You can also invoke both pow(2, 10)  and its alias my_pow(a, b) using SQL
        let sql_df = ctx.sql("SELECT greatest(2, 10), greatest(a, b) FROM t").await.unwrap();
        sql_df.show().await.unwrap();
    }
}
