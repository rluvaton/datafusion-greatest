#[cfg(test)]
mod scalars_tests {
    /// These tests check that calling greatest only on scalars values
    ///
    /// This does not include columns
    ///

    use crate::tests::utils::{create_context, create_empty_data_frame, get_result_as_matrix};
    use datafusion::arrow::datatypes::{Float32Type, Int8Type};
    use datafusion_common::ScalarValue;
    use datafusion_expr::lit;
    use crate::helpers::{Permutation, Transpose};

    #[tokio::test]
    async fn i8_without_nulls() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        let df = df.select(vec![greatest.call(vec![lit(2i8), lit(10i8)])]).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            vec![Some(10)]
        ]);
    }

    #[tokio::test]
    async fn i8_tests() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        // Only integers - 1, 2, 3 => 3
        let only_integers_permutations = vec![lit(1i8), lit(2i8), lit(3i8)].permutation(3);

        // 1 None, 2 integers - 1, None, 5 => 5
        let two_integers_one_none_permutations = vec![lit(1i8), lit(ScalarValue::Null), lit(5i8)].permutation(3);

        // 2 None, 1 integer - None, None, 1 => 1
        let two_none_one_integer_permutations = vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(1i8)].permutation(3);


        let df = df.select(vec![
            only_integers_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
            two_integers_one_none_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),
            two_none_one_integer_permutations.iter().map(|v| greatest.call(v.clone())).collect::<Vec<_>>(),

            // ###############################
            // Only none
            // ###############################

            // None, None, None => None
            vec![greatest.call(vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(ScalarValue::Null)])],


        ].concat()).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap().transpose();

        assert_eq!(results, vec![[
            vec![Some(3); only_integers_permutations.len()],
            vec![Some(5); two_integers_one_none_permutations.len()],
            vec![Some(1); two_none_one_integer_permutations.len()],
            vec![None]
        ].concat()]);
    }

    #[tokio::test]
    async fn f32_tests() {

        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        let df = df.select(vec![

            // ###############################
            // Only f32
            // ###############################

            // 1, 2, NAN => NAN
            greatest.call(vec![lit(1.0f32), lit(2.0f32), lit(f32::NAN)]),

            // NAN, 2, 1 => NAN
            greatest.call(vec![lit(f32::NAN), lit(2.0f32), lit(1.0f32)]),

            // NAN, 1, 2 => NAN
            greatest.call(vec![lit(f32::NAN), lit(1.0f32), lit(2.0f32)]),

            // 2, NAN, 1 => NAN
            greatest.call(vec![lit(2.0f32), lit(f32::NAN), lit(1.0f32)]),

            // ###############################
            // 1 None, 2 f32 and 1 NaN
            // ###############################

            // 1, None, NaN, 5 => 5
            greatest.call(vec![lit(1.0f32), lit(ScalarValue::Null), lit(5.0f32)]),

            // 1, None, 5 => 5
            greatest.call(vec![lit(1.0f32), lit(ScalarValue::Null), lit(5.0f32)]),

            // None, 1, 5 => 5
            greatest.call(vec![lit(ScalarValue::Null), lit(1.0f32), lit(5.0f32)]),

            // 1, 5, None => 5
            greatest.call(vec![lit(1.0f32), lit(5.0f32), lit(ScalarValue::Null)]),

            // 5, 1, None => 5
            greatest.call(vec![lit(5.0f32), lit(1.0f32), lit(ScalarValue::Null)]),

            // 5, None, 1 => 5
            greatest.call(vec![lit(5.0f32), lit(ScalarValue::Null), lit(1.0f32)]),


            // ###############################
            // 2 None, 1 f32
            // ###############################

            // None, None, 1 => 1
            greatest.call(vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(1.0f32)]),

            // None, 1, None => 1
            greatest.call(vec![lit(ScalarValue::Null), lit(1.0f32), lit(ScalarValue::Null)]),

            // 1, None, None => 1
            greatest.call(vec![lit(1.0f32), lit(ScalarValue::Null), lit(ScalarValue::Null)]),

            // ###############################
            // Only none
            // ###############################

            // None, None, None => None
            greatest.call(vec![lit(ScalarValue::Null), lit(ScalarValue::Null), lit(ScalarValue::Null)]),


        ]).unwrap();

        let results = get_result_as_matrix::<Float32Type>(df).await.unwrap().transpose();

        assert_eq!(results, vec![vec![
            Some(3.0),
            Some(3.0),
            Some(3.0),
            Some(3.0),

            Some(5.0),
            Some(5.0),
            Some(5.0),
            Some(5.0),
            Some(5.0),

            Some(1.0),
            Some(1.0),
            Some(1.0),

            None
        ]]);
    }
}
