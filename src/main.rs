//! Binary entry-point — delegates entirely to the library.

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let _ = rust_weather_api::build_rocket().launch().await?;
    Ok(())
}
