use hyper::Body;
use ipfs_api::{IpfsApi, IpfsClient, TryFromUri};
use json::{object, JsonValue};
use std::env;
use std::io::Cursor;

pub async fn handle_advance(
    client: &hyper::Client<hyper::client::HttpConnector>,
    _server_addr: &str,
    request: JsonValue,
) -> Result<&'static str, Box<dyn std::error::Error>> {
    println!("Received advance request data {}", &request);
    match env::var("LAMBADA_HTTP_SERVER_URL") {
        Ok(lambada_server_addr) => {
            let request = hyper::Request::builder()
                .method(hyper::Method::GET)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .uri(format!("{}/open_state", &lambada_server_addr))
                .body(Body::empty())?;

            let response = client.request(request).await;
            // Optional: Check if the request was successful
            if let Err(err) = response {
                return Err(format!("Failed to open state: {}", err).into());
            }
            println!("State opened successfully.");

            let ipfs_addr = env::var("IPFS_API").unwrap_or("http://127.0.0.1:5001".to_string());
            let ipfs_client = IpfsClient::from_str(&ipfs_addr).unwrap();

            // Creates a new directory only if such directory does not already exist.
            ipfs_client.files_mkdir("/state", false).await;

            let data = Cursor::new("hello world");
            ipfs_client
                .files_write("/state/output.file", true, true, data)
                .await
                .unwrap();

            let request = hyper::Request::builder()
                .method(hyper::Method::GET)
                .header(hyper::header::CONTENT_TYPE, "application/json")
                .uri(format!("{}/commit_state", &lambada_server_addr))
                .body(Body::empty())?;

            let response = client.request(request).await;

            if let Err(err) = response {
                println!("State committed err. {:?}", err);

                return Err(format!("Failed to commit state: {}", err).into());
            }
            // This will never show as we did the job and the runtime stopped us
            println!("State committed successfully.");
        }
        Err(_) => {}
    }
    Ok("accept")
}

pub async fn handle_inspect(
    _client: &hyper::Client<hyper::client::HttpConnector>,
    _server_addr: &str,
    request: JsonValue,
) -> Result<&'static str, Box<dyn std::error::Error>> {
    println!("Received inspect request data {}", &request);
    let _payload = request["data"]["payload"]
        .as_str()
        .ok_or("Missing payload")?;
    // TODO: add application logic here
    Ok("accept")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = hyper::Client::new();
    let server_addr = env::var("ROLLUP_HTTP_SERVER_URL")?;
    let mut status = "accept";
    loop {
        println!("Sending finish");
        let response = object! {"status" => status.clone()};
        let request = hyper::Request::builder()
            .method(hyper::Method::POST)
            .header(hyper::header::CONTENT_TYPE, "application/json")
            .uri(format!("{}/finish", &server_addr))
            .body(hyper::Body::from(response.dump()))?;

        let response = client.request(request).await?;
        println!("Received finish status {}", response.status());
        if response.status() == hyper::StatusCode::ACCEPTED {
            println!("No pending rollup request, trying again");
        } else {
            let body = hyper::body::to_bytes(response).await?;
            let utf = std::str::from_utf8(&body)?;
            let req = json::parse(utf)?;

            let request_type = req["request_type"]
                .as_str()
                .ok_or("request_type is not a string")?;
            status = match request_type {
                "advance_state" => handle_advance(&client, &server_addr[..], req).await?,
                "inspect_state" => handle_inspect(&client, &server_addr[..], req).await?,
                &_ => {
                    eprintln!("Unknown request type");
                    "reject"
                }
            };
        }
    }
}
