use std::collections::HashMap;
use tokio_postgres::{Client, Error, NoTls};
use std::fs::File;
use std::io::Read;
use std::vec::Vec;
use std::error::Error as StdError;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio_postgres::types::Type;
use log::{info, debug};
use chrono::NaiveDate;
use reqwest::Url;
use reqwest;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use chrono::DateTime;
use chrono::{Duration, Timelike, Utc};
use tokio::time;
use rust_decimal::Decimal;
use thousands::{Separable, SeparatorPolicy, digits};


struct Database {
    client: Client,
}

struct Matrix {
    
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum SqlValue {
    Int(i32),
    Bigint(i64),
    Text(String),
    // Date(NaiveDate),
    Numeric(Decimal),
    // Add more types here as needed.
}

#[derive(Debug, Deserialize)]
struct DatabaseConfig {
    hostaddr: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
    matrixuser: String,
    matrixpassword: String,
    matrixroom: String,
    matrixtoken: String,
}


#[derive(Serialize, Deserialize, Debug)]
struct MatrixAuth {
    user_id: String,
    access_token: String,
    home_server: String,
    device_id: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Blocks {
    epoch_no: i64,
    blocks_forged: i64,
}

#[derive(Serialize, Deserialize, Debug)]
struct Delegator {
    addr_view: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Address {
    stake_address: String,
    ada_value: Decimal,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {

    env_logger::init();

    let mut target_dt = get_prev_1_min_dt(Utc::now());
    let mut prevforged: Vec<HashMap<String, i64>> = vec![];
    let mut prevdelegators: Vec<HashMap<String, String>> = vec![];
    loop {


        // ESATBLISH CONNECTION TO POSTGRES DB INSTANCE
        
        target_dt = target_dt.checked_add_signed(Duration::seconds(60)).unwrap();
        let diff = (target_dt - Utc::now()).to_std().unwrap();
        time::sleep(diff).await;
        
        let db = Database::new().await?;
    
        db.ping().await?;


        //  GET LATEST BLOCK & DELEGATOR DATA

        let curforged = db.fetch_block_data("Select * from balance.bot_blocks_forged").await?;
        let curdelegators = db.fetch_delegator_data("Select * from balance.bot_delegator_list").await?;


        // PROCESS NEW BLOCK DATA

        if prevforged.is_empty() {
            prevforged = curforged.clone();
            println!("Startup block forge data loaded");
        } else {
            println!("Checking for block forge updates");
            let blockdiff: Vec<_> = curforged.into_iter().filter(|item| !prevforged.contains(item)).collect();

            if blockdiff.is_empty() {
                println!(" -- No new blocks forged");
                
            } else {
                println!(" -- New blocks forged!  Sending message....");

                let map =  serde_json::to_string(&blockdiff[0]).unwrap();
                let p: Blocks = serde_json::from_str(&map)?;
                
                let blockmsg: String = format!("{} of 23 blocks forged for epoch {}", p.blocks_forged, p.epoch_no);
                let forged = p.blocks_forged;

                Matrix::message(&blockmsg).await?;
                
                prevforged = blockdiff.clone();
                
            }

        }
        

        // PROCESS NEW DELEGATOR DATA
    
        if prevdelegators.is_empty() {
            prevdelegators = curdelegators.clone();
            println!("Startup delegator data loaded");

        } else {
            println!("Checking for delegator updates");

            let arrivals: Vec<_> = curdelegators.clone().into_iter().filter(|item| !prevdelegators.contains(item)).collect();
            let departures: Vec<_> = prevdelegators.clone().into_iter().filter(|item| !curdelegators.contains(item)).collect();

            if departures.is_empty() {
                println!(" -- No departures found");

            } else {

                println!(" -- New departures found....processing");

                Matrix::message("Delegator(s) leaving BALNC 🙏").await?;

                for (i, row_map) in departures.iter().enumerate() {

                    for (key, value) in row_map.iter() {

                        let addressquery: String = format!("Select * From api.bot_address_value('{}')", value);

                        let departuredata = db.fetch_address_data(&addressquery).await?;

                        let serialized = serde_json::to_string(&departuredata).unwrap();;
                        let deserialized: Vec<Address> = serde_json::from_str(&serialized).unwrap();

                        let policy = SeparatorPolicy {
                            separator: ',',
                            groups:    &[3],
                            digits:    digits::ASCII_DECIMAL,
                        };

                        let ada = deserialized[0].ada_value.separate_by_policy(policy);
                        let address = &deserialized[0].stake_address[..10];

                        // println!("{}  ❌ {} ₳", address, ada);

                        let departuresmsg: String = format!("❌ {} ₳ from {}  ", ada, address);

                        Matrix::message(&departuresmsg).await?;

                        prevdelegators = curdelegators.clone();

                        println!(" -- New departures send message complete");

                    }
                }

            }

            if arrivals.is_empty() {
                println!(" -- No arrivals found");

            } else {

                println!(" -- New arrivals found....processing");

                Matrix::message("Welcome new BALNC delegator(s)! 👏").await?;

                for (i, row_map) in arrivals.iter().enumerate() {

                    for (key, value) in row_map.iter() {

                        let addressquery: String = format!("Select * From api.bot_address_value('{}')", value);

                        let arrivaldata = db.fetch_address_data(&addressquery).await?;

                        let serialized = serde_json::to_string(&arrivaldata).unwrap();;
                        let deserialized: Vec<Address> = serde_json::from_str(&serialized).unwrap();

                        let policy = SeparatorPolicy {
                            separator: ',',
                            groups:    &[3],
                            digits:    digits::ASCII_DECIMAL,
                        };

                        let ada = deserialized[0].ada_value.separate_by_policy(policy);
                        let address = &deserialized[0].stake_address[..10];

                        // println!("{}  ✅ {} ₳", address, ada);

                        let arrivalsmsg: String = format!("✅ {} ₳ from {}  ", ada, address);

                        Matrix::message(&arrivalsmsg).await?;

                        prevdelegators = curdelegators.clone();

                        println!(" -- New arrivals send message complete");

                    }
                }

            }

        }

    }
}

fn read_config() -> Result<DatabaseConfig, Box<dyn std::error::Error>> {
    let mut file = File::open("database_config.yaml")?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let config: DatabaseConfig = serde_yaml::from_str(&contents)?;
    Ok(config)
}

fn get_prev_1_min_dt(current_dt: DateTime<Utc>) -> DateTime<Utc> {
    let target_dt = current_dt.checked_add_signed(Duration::seconds(1)).unwrap();
    let target_minute = (target_dt.minute() / 1) * 1;
    
    let target_dt = target_dt.with_minute(target_minute).unwrap();
    target_dt.with_second(0).unwrap()
}

impl Database {

    async fn new() -> Result<Self, Error> {
        let config = read_config().expect("Failed to read config");

        let connect_params = format!(
            "host={} port={} user={} password={} dbname={}",
            config.hostaddr, config.port, config.user, config.password, config.dbname
        );

        info!("Creating a new database instance");
        let (client, connection) = tokio_postgres::connect(
            &connect_params, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self { client })
    }

    async fn ping(&self) -> Result<(), Error> {
        match self.client.simple_query("SELECT 1;").await {
            Ok(_) => {
                println!("Successfully connected to the database.");
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn fetch_block_data(&self, query: &str) -> Result<Vec<HashMap<String, i64>>, Box<dyn std::error::Error>> {
        
        let rows = self.client.query(query, &[]).await?;

        let mut data: Vec<HashMap<String, i64>> = Vec::new();

        debug!("Query returned {} rows", rows.len());
        for row in rows.iter() {
            let mut row_map = HashMap::new();
            for column in row.columns() {
                let column_name = column.name();
                let type_ = column.type_();

                let value = if *type_ == Type::INT4 {
                    row.get(column_name)
                // } else if *type_ == Type::TEXT || *type_ == Type::VARCHAR {
                //     // Handle TEXT and VARCHAR types
                //     SqlValue::Text(row.get(column_name))
                // } else if *type_ == Type::DATE {
                //     // Handle DATE type
                //     SqlValue::Date(row.get(column_name))
                } else if *type_ == Type::INT8 {
                    row.get(column_name)
                } else {
                    // Handle other types or return an error.
                    continue;
                };
                row_map.insert(column_name.to_string(), value);
            }
            data.push(row_map);
        }
        
        Ok(data)
    }

    async fn fetch_delegator_data(&self, query: &str) -> Result<Vec<HashMap<String, String>>, Box<dyn std::error::Error>> {
        
        let rows = self.client.query(query, &[]).await?;

        let mut data: Vec<HashMap<String, String>> = Vec::new();

        debug!("Query returned {} rows", rows.len());
        for row in rows.iter() {
            let mut row_map = HashMap::new();
            for column in row.columns() {
                let column_name = column.name();
                let type_ = column.type_();

                let value = if *type_ == Type::INT4 {
                    row.get(column_name)
                } else if *type_ == Type::TEXT || *type_ == Type::VARCHAR {
                    // Handle TEXT and VARCHAR types
                    row.get(column_name)
                // } else if *type_ == Type::DATE {
                //     // Handle DATE type
                //     SqlValue::Date(row.get(column_name))
                } else if *type_ == Type::INT8 {
                    row.get(column_name)
                } else {
                    // Handle other types or return an error.
                    continue;
                };
                row_map.insert(column_name.to_string(), value);
            }
            data.push(row_map);
        }
        
        Ok(data)
    }

    async fn fetch_address_data(&self, query: &str) -> Result<Vec<HashMap<String, SqlValue>>, Box<dyn std::error::Error>> {
        
        let rows = self.client.query(query, &[]).await?;

        let mut data: Vec<HashMap<String, SqlValue>> = Vec::new();

        debug!("Query returned {} rows", rows.len());
        for row in rows.iter() {
            let mut row_map = HashMap::new();
            for column in row.columns() {
                let column_name = column.name();
                let type_ = column.type_();

                let value = if *type_ == Type::INT4 {
                    SqlValue::Int(row.get(column_name))
                } else if *type_ == Type::TEXT || *type_ == Type::VARCHAR {
                    SqlValue::Text(row.get(column_name))
                } else if *type_ == Type::INT8 {
                    SqlValue::Bigint(row.get(column_name))
                } else if *type_ == Type::NUMERIC {
                    SqlValue::Numeric(row.get(column_name))
                } else {
                    // Handle other types or return an error.
                    continue;
                };
                row_map.insert(column_name.to_string(), value);
            }
            data.push(row_map);
        }
        
        Ok(data)
    }

    
}

impl Matrix {

    async fn new() -> Result<MatrixAuth, Box<dyn StdError>> {
        let config = read_config().expect("Failed to read config");

        let mut map = HashMap::new();
        map.insert("type", "m.login.password");
        map.insert("user", &config.matrixuser);
        map.insert("password", &config.matrixpassword);
        map.insert("device_id", "balance_bot_service");

        let client = reqwest::Client::new();
        let response = client
            .post("https://matrix.forum.balanceanalytics.io/_matrix/client/r0/login")
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(&map)
            .send()
            .await
            .unwrap();

            let authdata: MatrixAuth = response.json::<MatrixAuth>().await?;
            println!("Matrix Access Token {}", authdata.access_token);

        Ok(authdata)
    }

    async fn message(query: &str) -> Result<(), Box<dyn StdError>> {
        let config = read_config().expect("Failed to read config");

        let mut url: String = format!("https://matrix.forum.balanceanalytics.io/_matrix/client/r0/rooms/{}/send/m.room.message?access_token={}", config.matrixroom, config.matrixtoken);

        let mut map = HashMap::new();
        map.insert("msgtype", "m.text");
        map.insert("body", query);

        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .header(ACCEPT, "application/json")
            .json(&map)
            .send()
            .await
            .unwrap();

        Ok(())
    }

}