/// Simple crate to access a influx database
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;

#[allow(unused)]
#[derive(Debug)]
pub enum Error {
    Response(reqwest::StatusCode),
    Reqwest(reqwest::Error),
    NoValueFound,
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Error::Reqwest(value)
    }
}

/// Physical Quantity handled by this crate
#[derive(Clone, Copy, Debug)]
pub enum PhysicalQuantity {
    Energy,
    Power,
    Battery,
}

impl PhysicalQuantity {
    /// Physical unit of the data
    pub fn unit(&self) -> &'static str {
        match self {
            PhysicalQuantity::Energy => "kWh",
            PhysicalQuantity::Power => "W",
            PhysicalQuantity::Battery => "%",
        }
    }

    /// Kind of data
    pub fn kind(&self) -> &'static str {
        match self {
            PhysicalQuantity::Energy => "energy",
            PhysicalQuantity::Power => "power",
            PhysicalQuantity::Battery => "battery",
        }
    }
}

pub struct InfluxDbConfig {
    pub host: String,
    pub db: String,
    pub user: String,
    pub pass: String,
    pub table: String,
}

#[allow(unused)]
#[derive(Deserialize, Debug, Clone)]
pub struct Value {
    time: DateTime<Utc>,
    value: f64,
    unit: String,
}

#[derive(Clone)]
pub struct InfluxDb {
    host: String,
    db: String,
    user: String,
    pass: String,
    table: String,
    client: Client,
}

impl InfluxDb {
    /// Create a niew client, Passwords are stored in plain text
    pub fn new(config: InfluxDbConfig) -> Self {
        let client = Client::new();
        InfluxDb {
            host: config.host,
            db: config.db,
            user: config.user,
            pass: config.pass,
            table: config.table,
            client,
        }
    }

    /// Get some values of a defined sensor and kind of date
    pub async fn get_values(
        &self,
        sensor_id: &str,
        quantity: PhysicalQuantity,
        count: u32,
    ) -> Result<Vec<Value>, Error> {
        // "SELECT * FROM sensor_data WHERE sensor_id='heatpump' LIMIT 10"
        let data = quantity.kind();
        let query = format!(
            "SELECT time,value,unit FROM {} WHERE sensor_id='{sensor_id}' AND data='{data}' ORDER BY DESC LIMIT {count}",
            self.table
        );
        let url = format!("{}/query?db={}&q={}", self.host, self.db, query);

        let response = self
            .client
            .get(&url)
            .basic_auth(self.user.to_owned(), Some(self.pass.to_owned())) // Authentifizierung via Basic Auth
            .send()
            .await
            .expect("Could not reach influx DB");

        let response: Results = if let Ok(response) = response.json().await {
            response
        } else {
            return Err(Error::NoValueFound);
        };
        let values = response.results[0].series[0].values.clone();
        Ok(values)
    }

    /// Store a value in the database, date and time will be added by influxdb
    pub async fn set_value(
        &self,
        sensor_id: &str,
        quantity: PhysicalQuantity,
        value: f64,
    ) -> Result<(), Error> {
        //"sensor_daten,raum=kueche temperatur=22.8"

        let unit = quantity.unit();
        let data = quantity.kind();
        let body = format!(
            "{},sensor_id={sensor_id},unit={unit},data={data} value={value}",
            self.table
        );
        let url = format!("{}/write?db={}", self.host, self.db);

        let response = self
            .client
            .post(&url)
            .basic_auth(self.user.to_owned(), Some(self.pass.to_owned())) // Authentifizierung via Basic Auth
            .body(body)
            .send()
            .await
            .expect("Could not reach influx DB");

        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::Response(response.status()))
        }
    }

    /// Get the last value of a sensor id and kind from database
    pub async fn get_value(
        &self,
        sensor_id: &str,
        quantity: PhysicalQuantity,
    ) -> Result<f64, Error> {
        let values = self.get_values(sensor_id, quantity, 1).await?;
        if !values.is_empty() {
            Ok(values[0].value)
        } else {
            Err(Error::NoValueFound)
        }
    }
}

// Structures to deserialize json data
#[allow(unused)]
#[derive(Deserialize, Debug, Clone)]
struct Results {
    results: Vec<Result_>,
}

#[allow(unused)]
#[derive(Deserialize, Debug, Clone)]
struct Result_ {
    statement_id: u32,
    series: Vec<Serie>,
}

#[allow(unused)]
#[derive(Deserialize, Debug, Clone)]
struct Serie {
    name: String,
    columns: Vec<String>,
    values: Vec<Value>,
}
