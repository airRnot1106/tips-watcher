use base64;
use chrono::{Duration, Utc};
use dotenv::dotenv;
use futures::stream::{self, StreamExt};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use percent_encoding::{utf8_percent_encode, AsciiSet};
use postgres_openssl::MakeTlsConnector;
use reqwest;
use reqwest::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE};
use std::collections::HashMap;
use std::env;
use std::process::Command;

struct Service {
    id: String,
    name: String,
    status: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let time = (Utc::now() + Duration::hours(9))
        .format("%Y年%m月%d日 %H:%M:%S")
        .to_string();
    println!("{}", time);
    let [tips, open_lms] = fetch_status_code().await?;
    println!("Tips: {}", tips.status);
    println!("Open LMS: {}", open_lms.status);
    let database_client = &connect_to_database().await?;
    let service_stream = stream::iter(vec![tips, open_lms]);
    service_stream
        .for_each(|service| async move {
            count_error_times(database_client, &service).await.unwrap();
            let tweet_type = should_tweet(database_client, &service).await.unwrap();
            if tweet_type == "DOWN" {
                tweet_service_down_alert(&service).await.unwrap();
            } else if tweet_type == "UP" {
                tweet_service_up_alert(&service).await.unwrap();
            };
        })
        .await;

    Ok(())
}

fn get_reqest(url: &str) -> i32 {
    let output = Command::new("curl")
        .args(&[url, "-o", "/dev/null", "-w", "%{http_code}", "-s", "-k"])
        .output()
        .expect("failed to start `curl`");
    let status_code = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<i32>()
        .unwrap();
    println!("{}", status_code);
    status_code
}

async fn fetch_status_code() -> Result<[Service; 2], Box<dyn std::error::Error>> {
    const TIPS_URL: &str = "https://tips.u-tokai.ac.jp/campusweb/";
    let tips_status = get_reqest(TIPS_URL);
    let tips = Service {
        id: String::from("S_TIPS"),
        name: String::from("TIPS"),
        status: tips_status,
    };
    const OPEN_LMS_URL: &str = "https://tlms.tsc.u-tokai.ac.jp/";
    let open_lms_status = get_reqest(OPEN_LMS_URL);
    let open_lms = Service {
        id: String::from("S_OPENLMS"),
        name: String::from("Open LMS"),
        status: open_lms_status,
    };
    Ok([tips, open_lms])
}

async fn connect_to_database() -> Result<tokio_postgres::Client, Box<dyn std::error::Error>> {
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut builder =
        SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder
        .set_ca_file("tmp/ca.crt")
        .expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (client, connection) = tokio_postgres::connect(&database_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

async fn count_error_times(
    client: &tokio_postgres::Client,
    service: &Service,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = if service.status >= 500 && service.status < 600 {
        format!("UPDATE services SET count_of_connection_failure = count_of_connection_failure + 1 WHERE id = '{}'",service.id)
    } else {
        format!(
            "UPDATE services SET count_of_connection_failure = 0 WHERE id = '{}'",
            service.id
        )
    };
    client.execute(&query, &[]).await?;
    Ok(())
}

async fn is_down(
    client: &tokio_postgres::Client,
    service: &Service,
) -> Result<bool, Box<dyn std::error::Error>> {
    let query = format!(
        "SELECT count_of_connection_failure FROM services WHERE id = '{}'",
        service.id
    );
    let rows = client.query(&query, &[]).await?;
    let count_of_connection_failure: i32 = rows[0].get(0);
    Ok(count_of_connection_failure > 2)
}

async fn should_tweet(
    client: &tokio_postgres::Client,
    service: &Service,
) -> Result<String, Box<dyn std::error::Error>> {
    let query = format!("SELECT is_down FROM services WHERE id = '{}'", service.id);
    let rows = client.query(&query, &[]).await?;
    let last_is_down: bool = rows[0].get(0);
    let current_is_down = is_down(client, service).await?;
    let state_array = [last_is_down, current_is_down];
    if !state_array[0] && state_array[1] {
        let query = format!(
            "UPDATE services SET is_down = true WHERE id = '{}'",
            service.id
        );
        client.execute(&query, &[]).await?;
        return Ok(String::from("DOWN"));
    } else if state_array[0] && !state_array[1] {
        let query = format!(
            "UPDATE services SET is_down = false WHERE id = '{}'",
            service.id
        );
        client.execute(&query, &[]).await?;
        return Ok(String::from("UP"));
    } else {
        return Ok(String::from("NONE"));
    };
}

struct Twitter {
    consumer_key: String,
    consumer_secret: String,
    access_token_key: String,
    access_token_secret: String,
}

impl Twitter {
    const FRAGMENT: AsciiSet = percent_encoding::NON_ALPHANUMERIC
        .remove(b'*')
        .remove(b'-')
        .remove(b'.')
        .remove(b'_');

    fn new(
        consumer_key: String,
        consumer_secret: String,
        access_token_key: String,
        access_token_secret: String,
    ) -> Twitter {
        Twitter {
            consumer_key,
            consumer_secret,
            access_token_key,
            access_token_secret,
        }
    }

    fn get_request_header(&self, method: &str, endpoint: &str) -> HeaderMap {
        let timestamp = format!("{}", Utc::now().timestamp());

        let mut oauth_params: HashMap<&str, &str> = HashMap::new();
        oauth_params.insert("oauth_nonce", &timestamp);
        oauth_params.insert("oauth_timestamp", &timestamp);
        oauth_params.insert("oauth_signature_method", "HMAC-SHA1");
        oauth_params.insert("oauth_version", "1.0");
        oauth_params.insert("oauth_consumer_key", &self.consumer_key);
        oauth_params.insert("oauth_token", &self.access_token_key);

        let signature_key =
            &self.generate_oauth_signature_key(&self.consumer_secret, &self.access_token_secret);
        let signature_base = &self.generate_oauth_signature_base(method, endpoint, &oauth_params);

        let signature = &self.generate_oauth_signature(signature_key, signature_base);
        oauth_params.insert("oauth_signature", &signature);

        let mut headers = reqwest::header::HeaderMap::new();

        let oauth_header = format!(
            "OAuth {}",
            oauth_params
                .iter()
                .map(|(key, value)| {
                    format!(
                        r#"{}="{}""#,
                        utf8_percent_encode(key, &Self::FRAGMENT),
                        utf8_percent_encode(value, &Self::FRAGMENT)
                    )
                })
                .collect::<Vec<String>>()
                .join(", ")
        );

        headers.insert(AUTHORIZATION, oauth_header.parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        headers
    }

    fn generate_oauth_signature_key(
        &self,
        consumer_secret: &str,
        access_token_secret: &str,
    ) -> String {
        let key: String = format!(
            "{}&{}",
            utf8_percent_encode(consumer_secret, &Self::FRAGMENT),
            utf8_percent_encode(access_token_secret, &Self::FRAGMENT)
        );

        key
    }

    fn generate_oauth_param_string(&self, params: &HashMap<&str, &str>) -> String {
        let mut params: Vec<(&&str, &&str)> = params.iter().collect();
        params.sort();

        let param_string = params
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    utf8_percent_encode(key, &Self::FRAGMENT),
                    utf8_percent_encode(value, &Self::FRAGMENT)
                )
            })
            .collect::<Vec<String>>()
            .join("&");

        param_string
    }

    fn generate_oauth_signature_base(
        &self,
        method: &str,
        endpoint: &str,
        params: &HashMap<&str, &str>,
    ) -> String {
        let param_string = &self.generate_oauth_param_string(params);

        let base = format!(
            "{}&{}&{}",
            utf8_percent_encode(method, &Self::FRAGMENT),
            utf8_percent_encode(endpoint, &Self::FRAGMENT),
            utf8_percent_encode(param_string, &Self::FRAGMENT)
        );

        base
    }

    fn generate_oauth_signature(&self, signature_key: &str, signature_base: &str) -> String {
        let hash = hmacsha1::hmac_sha1(signature_key.as_bytes(), signature_base.as_bytes());
        let signature = base64::encode(hash);

        signature
    }

    async fn tweet(&self, text: &str) -> Result<(), Box<dyn std::error::Error>> {
        const HTTP_METHOD: &str = "POST";
        const TWEET_ENDPOINT: &str = "https://api.twitter.com/2/tweets";

        let headers = self.get_request_header(HTTP_METHOD, TWEET_ENDPOINT);

        let mut content: HashMap<&str, &str> = HashMap::new();
        content.insert("text", text);

        let client = reqwest::Client::new();
        let res = client
            .post(TWEET_ENDPOINT)
            .headers(headers)
            .json(&content)
            .send()
            .await?;
        if res.status().is_success() {
            Ok(())
        } else {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{}", res.status()),
            )))
        }
    }
}

async fn tweet_service_down_alert(service: &Service) -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let time = (Utc::now() + Duration::hours(9))
        .format("%Y年%m月%d日 %H:%M:%S")
        .to_string();
    let text = format!("【障害情報】\n{}\n現在、{}に繋がりにくくなっています。\nサーバが復旧したら再度お知らせします。", time, service.name);
    println!("{}", text);

    let twitter = Twitter::new(
        env::var("TWITTER_API_KEY").unwrap(),
        env::var("TWITTER_API_SECRET").unwrap(),
        env::var("TWITTER_API_ACCESS_TOKEN").unwrap(),
        env::var("TWITTER_API_ACCESS_SECRET").unwrap(),
    );
    twitter.tweet(&text).await?;
    Ok(())
}

async fn tweet_service_up_alert(service: &Service) -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let time = (Utc::now() + Duration::hours(9))
        .format("%Y年%m月%d日 %H:%M:%S")
        .to_string();
    let text = format!(
        "【復旧情報】\n{}\n{}のサーバが復旧しました。",
        time, service.name
    );
    println!("{}", text);

    let twitter = Twitter::new(
        env::var("TWITTER_API_KEY").unwrap(),
        env::var("TWITTER_API_SECRET").unwrap(),
        env::var("TWITTER_API_ACCESS_TOKEN").unwrap(),
        env::var("TWITTER_API_ACCESS_SECRET").unwrap(),
    );
    twitter.tweet(&text).await?;
    Ok(())
}
