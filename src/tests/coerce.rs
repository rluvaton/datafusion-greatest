
#[cfg(test)]
mod coerce_tests {
    use crate::helpers::Permutation;
    use crate::tests::utils::{create_context, create_empty_data_frame, get_combined_results, parse_many_columns};
    use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};
    use datafusion_expr::lit;

    #[tokio::test]
    async fn coerce_types() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // Testing permutation of all types to make sure that the order of the arguments does not matter
        let i8 = vec![lit(1i8), lit(2i8)].permutation(2);
        let i16 = vec![lit(1i8), lit(2i16)].permutation(2);
        let i32 = vec![lit(1i8), lit(2i16), lit(2i32)].permutation(3);
        let i64 = vec![lit(1i8), lit(2i16), lit(2i32), lit(3i64)].permutation(4);
        let f32 = vec![lit(1i8), lit(2i16), lit(2i32), lit(3i64), lit(1f32)].permutation(5);
        let f64 = vec![lit(1i8), lit(2i16), lit(2i32), lit(3i64), lit(1f32), lit(2f64)].permutation(6);
        let greatest_calls = vec![
            i8.clone(),
            i16.clone(),
            i32.clone(),
            i64.clone(),
            f32.clone(),
            f64.clone(),
        ].concat().iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>();

        let df = df.select(greatest_calls).unwrap();

        let results = get_combined_results(df).await.unwrap();
        let columns: Vec<_> = results.columns().iter().collect();


        let (i8_results, columns) = columns.split_at(i8.len());
        parse_many_columns::<Int8Type>(i8_results);

        let (i16_results, columns) = columns.split_at(i16.len());
        parse_many_columns::<Int16Type>(i16_results);

        let (i32_results, columns) = columns.split_at(i32.len());
        parse_many_columns::<Int32Type>(i32_results);

        let (i64_results, columns) = columns.split_at(i64.len());
        parse_many_columns::<Int64Type>(i64_results);

        let (f32_results, columns) = columns.split_at(f32.len());
        parse_many_columns::<Float32Type>(f32_results);

        let (f64_results, columns) = columns.split_at(f64.len());
        parse_many_columns::<Float64Type>(f64_results);

        // If this failed it means that we forgot to assert some columns
        assert_eq!(columns.len(), 0, "There should be no more columns left in the results");
    }

}
