#[cfg(test)]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};
    use crate::tests::utils::{create_context, create_primitive_array, find_greatest, generate_optional_values, get_result_as_matrix};
    use datafusion::arrow::array::ArrayRef;
    use datafusion_expr::col;
    use rand::Rng;

    #[tokio::test]
    async fn i8_without_nulls() {
        let (ctx, greatest) = create_context();
        let mut rng = rand::thread_rng();

        let a_vec = Vec::from_iter((0..8).map(|_| Some(rng.gen::<i8>())));
        let b_vec = Vec::from_iter((0..8).map(|_| Some(rng.gen::<i8>())));
        let a: ArrayRef = create_primitive_array::<Int8Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int8Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let df = ctx.table("t").await.unwrap();

        let df = df.clone().select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn i8_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = vec![Some(1), None, Some(-10), Some(4), None, Some(120), Some(7), Some(30)];
        let b_vec = vec![Some(5), None, None, Some(-2), Some(10), Some(1), Some(23), None];
        let a: ArrayRef = create_primitive_array::<Int8Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int8Type>(b_vec.clone());
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let df = ctx.table("t").await.unwrap();

        let df = df.clone().select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

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

        let df = df.clone().select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int16Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn i16_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<i16>(100, Some(0.5));
        let b_vec = generate_optional_values::<i16>(100, Some(0.5));
        let a: ArrayRef = create_primitive_array::<Int16Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int16Type>(b_vec.clone());
        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        // declare a table in memory. In Spark API, this corresponds to createDataFrame(...).
        ctx.register_batch("t", batch).unwrap();

        // get a DataFrame from the context for scanning the "t" table
        let df = ctx.table("t").await.unwrap();

        let df = df.clone().select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int16Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn i32_without_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<i32>(100, Some(0.0));
        let b_vec = generate_optional_values::<i32>(100, Some(0.0));
        let a: ArrayRef = create_primitive_array::<Int32Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int32Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn i32_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<i32>(100, Some(0.5));
        let b_vec = generate_optional_values::<i32>(100, Some(0.5));
        let a: ArrayRef = create_primitive_array::<Int32Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int32Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn i64_without_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<i64>(100, Some(0.0));
        let b_vec = generate_optional_values::<i64>(100, Some(0.0));
        let a: ArrayRef = create_primitive_array::<Int64Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int64Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int64Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn i64_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<i64>(100, Some(0.5));
        let b_vec = generate_optional_values::<i64>(100, Some(0.5));
        let a: ArrayRef = create_primitive_array::<Int64Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Int64Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Int64Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn f32_without_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<f32>(100, Some(0.0));
        let b_vec = generate_optional_values::<f32>(100, Some(0.0));
        let a: ArrayRef = create_primitive_array::<Float32Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Float32Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Float32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn f32_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<f32>(100, Some(0.5));
        let b_vec = generate_optional_values::<f32>(100, Some(0.5));
        let a: ArrayRef = create_primitive_array::<Float32Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Float32Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Float32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn f64_without_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<f64>(100, Some(0.0));
        let b_vec = generate_optional_values::<f64>(100, Some(0.0));
        let a: ArrayRef = create_primitive_array::<Float64Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Float64Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Float64Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn f64_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_optional_values::<f64>(100, Some(0.5));
        let b_vec = generate_optional_values::<f64>(100, Some(0.5));
        let a: ArrayRef = create_primitive_array::<Float64Type>(a_vec.clone());
        let b: ArrayRef = create_primitive_array::<Float64Type>(b_vec.clone());

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_result_as_matrix::<Float64Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }
}
