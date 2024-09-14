use std::ops::Range;
use datafusion::arrow::array::{PrimitiveArray, StringArray};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::ArrowPrimitiveType;
use rand::distributions::{Alphanumeric, Standard};
use rand::{thread_rng, Rng, distributions::Distribution};
use std::sync::Arc;

pub(crate) fn create_primitive_array<T: ArrowPrimitiveType>(values: Vec<Option<T::Native>>) -> Arc<PrimitiveArray<T>> {
    // true for when value is not null
    let nulls = NullBuffer::from(values.iter().map(|x| x.is_some()).collect::<Vec<bool>>());

    Arc::new(PrimitiveArray::<T>::new(
        values.into_iter().map(|x| x.unwrap_or_else(T::default_value)).collect(), Some(nulls),
    ))
}

pub(crate) fn create_string_array(values: Vec<Option<String>>) -> Arc<StringArray> {
    Arc::new(StringArray::from(values))
}

pub(crate) fn generate_values<T>(size: usize) -> Vec<T> where Standard: Distribution<T> {
    let mut rng = thread_rng();
    Vec::from_iter((0..size).map(|_| rng.gen::<T>()))
}

pub(crate) fn generate_optional_values<T>(size: usize, probability_of_none: Option<f64>) -> Vec<Option<T>> where Standard: Distribution<T> {
    let mut rng = thread_rng();

    Vec::from_iter((0..size).map(|_| {
        if rng.gen_bool(probability_of_none.unwrap_or(0.5)) {
            None
        } else {
            Some(rng.gen::<T>())
        }
    }))
}

pub(crate) fn generate_string_values(size: usize, length_range: Range<usize>, probability_of_none: Option<f64>) -> Vec<Option<String>> {
    let mut rng = thread_rng();

    Vec::from_iter((0..size).map(|_| {
        if rng.gen_bool(probability_of_none.unwrap_or(0.5)) {
            None
        } else {
            let rand_string: String = rng.clone()
                .sample_iter(&Alphanumeric)
                .take(rng.gen_range(length_range.clone()))
                .map(char::from)
                .collect();

            Some(rand_string)
        }
    }))
}

pub(crate) fn generate_list_values<T>(size: usize, list_length_range: Range<usize>, list_probability_of_none: Option<f64>, list_item_probability_of_none: Option<f64>) -> Vec<Option<Vec<Option<T>>>> where Standard: Distribution<T> {
    let mut rng = thread_rng();

    Vec::from_iter((0..size).map(|_| {
        if rng.gen_bool(list_probability_of_none.unwrap_or(0.5)) {
            None
        } else {
            Some(generate_optional_values::<T>(rng.gen_range(list_length_range.clone()), list_item_probability_of_none))
        }
    }))
}


