#[cfg(test)]
mod tests {
    use crate::tests::utils::{create_context, create_primitive_array, find_greatest, generate_list_values, generate_optional_values, generate_string_values, get_list_result_as_matrix, get_primitive_result_as_matrix, get_string_result_as_matrix};
    use datafusion::arrow::array::{ArrayRef, ListArray, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};
    use datafusion_expr::col;
    use rand::Rng;
    use std::sync::Arc;
    use crate::helpers::Transpose;

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

        let results = get_primitive_result_as_matrix::<Int8Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int8Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int16Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int16Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int32Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int32Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int64Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Int64Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Float32Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Float32Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Float64Type>(df).await.unwrap();

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

        let results = get_primitive_result_as_matrix::<Float64Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn string_without_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_string_values(100, 1..20, Some(0.0));
        let b_vec = generate_string_values(100, 1..20, Some(0.0));
        let a: ArrayRef = Arc::new(StringArray::from(a_vec.clone()));
        let b: ArrayRef = Arc::new(StringArray::from(b_vec.clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_string_result_as_matrix(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn string_with_nulls() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_string_values(100, 1..20, Some(0.5));
        let b_vec = generate_string_values(100, 1..20, Some(0.5));
        let a: ArrayRef = Arc::new(StringArray::from(a_vec.clone()));
        let b: ArrayRef = Arc::new(StringArray::from(b_vec.clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_string_result_as_matrix(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn list_without_nulls_in_nulls_as_list_and_in_values() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_list_values(100, 1..20, Some(0.0), Some(0.0));
        let b_vec = generate_list_values(100, 1..20, Some(0.0), Some(0.0));
        let a: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(a_vec.clone()));
        let b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(b_vec.clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_list_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn list_with_nulls_as_list_and_in_list_items() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_list_values(100, 1..20, Some(0.5), Some(0.5));
        let b_vec = generate_list_values(100, 1..20, Some(0.5), Some(0.5));
        let a: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(a_vec.clone()));
        let b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(b_vec.clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_list_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn list_with_nulls_as_list() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_list_values(100, 1..20, Some(0.5), Some(0.0));
        let b_vec = generate_list_values(100, 1..20, Some(0.5), Some(0.0));
        let a: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(a_vec.clone()));
        let b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(b_vec.clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_list_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn list_with_nulls_as_list_items() {
        let (ctx, greatest) = create_context();

        let a_vec = generate_list_values(100, 1..20, Some(0.0), Some(0.5));
        let b_vec = generate_list_values(100, 1..20, Some(0.0), Some(0.5));
        let a: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(a_vec.clone()));
        let b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(b_vec.clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_list_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![
            find_greatest(vec![a_vec.clone(), b_vec.clone()])
        ]);
    }

    #[tokio::test]
    async fn lists() {
        let (ctx, greatest) = create_context();
        let rows = vec![
            vec![
                // Greatest is 2 as we look at the first item in each list first
                Some(vec![Some(2), Some(100)]),
                Some(vec![Some(1), Some(200)]),
            ],
            vec![
                // Greatest is 1 as 1 is greater than None
                Some(vec![None, Some(100)]),
                Some(vec![Some(1), Some(200)]),
            ],
            vec![
                // Greatest is 200 as if the first item is equal we look at the second item
                Some(vec![Some(6), Some(100)]),
                Some(vec![Some(6), Some(200)]),
            ],
            vec![
                // Greatest is 200 as if the first item is equal we look at the second item
                Some(vec![None, Some(100)]),
                Some(vec![None, Some(200)]),
            ],
            vec![
                // Greatest is None as having a value is greater than no value
                Some(vec![None]),
                Some(vec![]),
            ],
            vec![
                // Greatest is 1 as 1 is greater than 0, even though the length is different
                Some(vec![Some(1)]),
                Some(vec![Some(0), Some(4)]),
            ],
            vec![
                // Greatest is 4 as 4 is greater than nothing
                Some(vec![Some(0)]),
                Some(vec![Some(0), Some(4)]),
            ],
            vec![
                // Greatest is 0 as 0 is greater than None
                Some(vec![None, Some(3)]),
                Some(vec![Some(0)]),
            ],
        ];

        let cols = rows.transpose();

        let a: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(cols[0].clone()));
        let b: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(cols[1].clone()));

        let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        ctx.register_batch("t", batch).unwrap();


        let df = ctx.table("t").await.unwrap();

        let df = df.select(vec![greatest.call(vec![col("a"), col("b")])]).unwrap();

        let results = get_list_result_as_matrix::<Int32Type>(df).await.unwrap();

        assert_eq!(results, vec![vec![
            Some(vec![Some(2), Some(100)]),
            Some(vec![Some(1), Some(200)]),
            Some(vec![Some(6), Some(200)]),
            Some(vec![None, Some(200)]),
            Some(vec![None]),
            Some(vec![Some(1)]),
            Some(vec![Some(0), Some(4)]),
            Some(vec![Some(0)]),
        ]]);
    }
}
