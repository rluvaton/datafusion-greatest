use datafusion::arrow::array::PrimitiveArray;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::ArrowPrimitiveType;
use rand::distributions::Standard;
use rand::Rng;
use std::sync::Arc;

pub(crate) fn create_primitive_array<T: ArrowPrimitiveType>(values: Vec<Option<T::Native>>) -> Arc<PrimitiveArray<T>> {
    // true for when value is not null
    let nulls = NullBuffer::from(values.iter().map(|x| x.is_some()).collect::<Vec<bool>>());

    Arc::new(PrimitiveArray::<T>::new(
        values.into_iter().map(|x| x.unwrap_or_else(T::default_value)).collect(), Some(nulls),
    ))
}

pub(crate) fn generate_values<T>(size: usize) -> Vec<T> where Standard: rand::distributions::Distribution<T> {
    let mut rng = rand::thread_rng();
    Vec::from_iter((0..size).map(|_| rng.gen::<T>()))
}

pub(crate) fn generate_optional_values<T>(size: usize, probability_of_none: Option<f64>) -> Vec<Option<T>> where Standard: rand::distributions::Distribution<T> {
    let mut rng = rand::thread_rng();

    Vec::from_iter((0..size).map(|_| {
        if rng.gen_bool(probability_of_none.unwrap_or(0.5)) {
            Some(rng.gen::<T>())
        } else {
            None
        }
    }))
}
