
#[cfg(test)]
mod coerce_tests {
    use crate::helpers::Permutation;
    use crate::tests::utils::{create_context, create_empty_data_frame, get_combined_results, parse_many_primitives_columns};
    use crate::vec_with_lit;
    use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type};

    #[tokio::test]
    async fn coerce_types() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // Testing permutation of all types to make sure that the order of the arguments does not matter
        let i8 = vec_with_lit![1i8, 2i8].permutation(2);
        let i16 = vec_with_lit![1i8, 2i16].permutation(2);
        let i32 = vec_with_lit![1i8, 2i16, 2i32].permutation(3);
        let i64 = vec_with_lit![1i8, 2i16, 2i32, 3i64].permutation(4);
        let f32 = vec_with_lit![1i8, 2i16, 2i32, 3i64, 1f32].permutation(5);
        let f64 = vec_with_lit![1i8, 2i16, 2i32, 3i64, 1f32, 2f64].permutation(6);
        let greatest_calls = vec![
            i8.clone(),
            i16.clone(),
            i32.clone(),
            i64.clone(),
            f32.clone(),
            f64.clone()
        ].concat().iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>();

        let df = df.select(greatest_calls).unwrap();

        let results = get_combined_results(df).await.unwrap();
        let columns: Vec<_> = results.columns().iter().collect();


        let (i8_results, columns) = columns.split_at(i8.len());
        parse_many_primitives_columns::<Int8Type>(i8_results);

        let (i16_results, columns) = columns.split_at(i16.len());
        parse_many_primitives_columns::<Int16Type>(i16_results);

        let (i32_results, columns) = columns.split_at(i32.len());
        parse_many_primitives_columns::<Int32Type>(i32_results);

        let (i64_results, columns) = columns.split_at(i64.len());
        parse_many_primitives_columns::<Int64Type>(i64_results);

        let (f32_results, columns) = columns.split_at(f32.len());
        parse_many_primitives_columns::<Float32Type>(f32_results);

        let (f64_results, columns) = columns.split_at(f64.len());
        parse_many_primitives_columns::<Float64Type>(f64_results);

        // If this failed it means that we forgot to assert some columns
        assert_eq!(columns.len(), 0, "There should be no more columns left in the results");
    }


    #[tokio::test]
    async fn have_incompatible_type() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // Testing permutation of all types to make sure that the order of the arguments does not matter

        // i8 and u8 are incompatible but i16 contain both
        let i8_and_u8 = vec_with_lit![1i8, 2u8, 3i16].permutation(3);
        // i16 and u16 are incompatible but i32 contain both
        let i16_and_u16 = vec_with_lit![1i16, 2u16, 3i32].permutation(3);
        // i32 and u32 are incompatible but i64 contain both
        let i32_and_u32 = vec_with_lit![1i32, 2u32, 3i64].permutation(3);
        // i64 and u64 are incompatible but f32 contain both
        let i64_and_u64_with_f32 = vec_with_lit![1i64, 2u64, 3f32].permutation(3);
        // i64 and u64 are incompatible but f64 contain both
        let i64_and_u64_with_f64 = vec_with_lit![1i64, 2u64, 3f64].permutation(3);
        let greatest_calls = vec![
            i8_and_u8.clone(),
            i16_and_u16.clone(),
            i32_and_u32.clone(),
            i64_and_u64_with_f32.clone(),
            i64_and_u64_with_f64.clone()
        ].concat().iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>();

        let df = df.select(greatest_calls).unwrap();

        let results = get_combined_results(df).await.unwrap();
        let columns: Vec<_> = results.columns().iter().collect();

        let (i16_results, columns) = columns.split_at(i8_and_u8.len());
        parse_many_primitives_columns::<Int16Type>(i16_results);

        let (i32_results, columns) = columns.split_at(i16_and_u16.len());
        parse_many_primitives_columns::<Int32Type>(i32_results);

        let (i64_results, columns) = columns.split_at(i32_and_u32.len());
        parse_many_primitives_columns::<Int64Type>(i64_results);

        let (f32_results, columns) = columns.split_at(i64_and_u64_with_f32.len());
        parse_many_primitives_columns::<Float32Type>(f32_results);

        let (f64_results, columns) = columns.split_at(i64_and_u64_with_f64.len());
        parse_many_primitives_columns::<Float64Type>(f64_results);

        // If this failed it means that we forgot to assert some columns
        assert_eq!(columns.len(), 0, "There should be no more columns left in the results");
    }

}
