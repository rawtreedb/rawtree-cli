use anyhow::Result;

use crate::client::ApiClient;

pub fn docs(client: &ApiClient) -> Result<()> {
    let text = client.get_raw("/v1/docs")?;
    print!("{}", text);
    Ok(())
}
